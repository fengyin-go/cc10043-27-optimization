"""交付凭据台账：上游交付的幂等接收、签收凭据保留与可消费输入选择。

设计要点
--------
- 幂等键是 ``(来源标识, 交付编号)``，而不是内容指纹：因此另一次真实请求即使
  内容碰巧相同，只要交付编号不同，仍然是一笔新交付。
- 连接器首次接收、发送方重送、崩溃后的恢复入口都必须经过 :meth:`receive`，
  台账在同一把锁内完成「查台账 -> 判重/判冲突 -> 落载荷 -> 签收」，任何入口
  对同一交付的判断结果都一致。
- 同键同内容返回首张签收凭据；同键异内容保留首次接收的原始数据并登记冲突，
  冲突载荷永远不会覆盖或重复写入。
- 消息流源只从已签收凭据中选择实际可消费的输入，并把原交付编号与收到的内容
  对应起来。
"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

RECEIPT_TABLE = "delivery_receipt"
PAYLOAD_TABLE = "payload_store"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_content(payload: Any) -> str:
    """把载荷规范化为可比较、可持久化的字符串（字典按键排序，消除排版差异）。"""
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class DeliveryLedgerService:
    """以（来源标识, 交付编号）为幂等键的交付台账，所有接收入口的唯一裁决者。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()

    # ---- 基础查询 ----

    def _rows(self) -> list[dict[str, Any]]:
        return store.rows(RECEIPT_TABLE)

    def find_receipt(self, source_key: str, delivery_id: str) -> dict[str, Any] | None:
        for row in self._rows():
            if row.get("来源标识") == source_key and row.get("交付编号") == delivery_id:
                return row
        return None

    def get_by_receipt_no(self, receipt_no: str) -> dict[str, Any] | None:
        for row in self._rows():
            if row.get("签收凭据") == receipt_no:
                return row
        return None

    def list_receipts(self, source_key: str | None = None) -> list[dict[str, Any]]:
        rows = self._rows()
        if source_key is not None:
            rows = [row for row in rows if row.get("来源标识") == source_key]
        return list(rows)

    # ---- 核心：统一接收裁决（首收 / 重送 / 恢复共用） ----

    def receive(
        self,
        source_key: str,
        delivery_id: str,
        payload: Any,
        *,
        topic: str = "",
    ) -> dict[str, Any]:
        """接收一次上游交付，返回裁决结果。

        ``outcome`` 取值：

        - ``accepted``：全新交付，已落载荷并签发首张凭据；
        - ``duplicate``：同源同交付编号且内容一致，返回原凭据，不重复写入；
        - ``conflict``：同交付编号内容发生变化，保留原数据并告知冲突。
        """
        if not isinstance(delivery_id, str) or not delivery_id.strip():
            return {"ok": False, "outcome": "invalid", "message": "交付编号不能为空"}
        content = canonical_content(payload)
        digest = content_hash(content)

        # 整块临界区：查台账、判重、落载荷、签收据在同一把锁内原子完成，
        # 并发重送与恢复重放不会产生重复交付。
        with self._lock:
            existing = self.find_receipt(source_key, delivery_id)
            if existing is not None:
                existing["最近重发时间"] = _now()
                if existing["内容指纹"] == digest:
                    existing["重发次数"] = int(existing.get("重发次数", 0)) + 1
                    return {
                        "ok": True,
                        "outcome": "duplicate",
                        "receipt": existing,
                        "message": "同源同交付编号内容一致，返回首张签收凭据",
                    }
                # 内容变了：原数据原封不动，只登记冲突事件。
                conflict_event = {
                    "冲突时间": _now(),
                    "原内容指纹": existing["内容指纹"],
                    "新来内容指纹": digest,
                    "新来内容": content,
                }
                existing["冲突次数"] = int(existing.get("冲突次数", 0)) + 1
                existing.setdefault("冲突记录", []).append(conflict_event)
                return {
                    "ok": False,
                    "outcome": "conflict",
                    "receipt": existing,
                    "conflict": conflict_event,
                    "message": "同交付编号内容已变化，保留首次接收的数据并判定冲突",
                }

            # 全新交付：先落原始载荷，再签签收凭据。
            message_id = f"{source_key}:{delivery_id}"
            payload_rows = store.rows(PAYLOAD_TABLE)
            payload_pk = max((int(row.get("id", 0)) for row in payload_rows), default=0) + 1
            payload_rows.append({
                "id": payload_pk,
                "采集批次": topic or source_key,
                "消息ID": message_id,
                "原始载荷": content,
                "存储时间": _now(),
                "压缩后大小": str(len(content)),
                "status": "已存储",
                "pending": True,
                "abnormal": False,
            })

            seq = len(self._rows()) + 1
            receipt_no = f"RCV-{seq:06d}"
            receipt = {
                "id": seq,
                "来源标识": source_key,
                "交付编号": delivery_id,
                "Topic": topic or source_key,
                "内容指纹": digest,
                "原始内容": content,
                "载荷消息ID": message_id,
                "载荷记录ID": payload_pk,
                "签收凭据": receipt_no,
                "首次接收时间": _now(),
                "最近重发时间": None,
                "重发次数": 0,
                "冲突次数": 0,
                "冲突记录": [],
                "确认送达": True,
                "已消费": False,
                "消费时间": None,
                "status": "已签收",
            }
            self._rows().append(receipt)
            return {
                "ok": True,
                "outcome": "accepted",
                "receipt": receipt,
                "message": "交付已首次接收并签收",
            }

    def mark_ack_lost(self, receipt_no: str) -> dict[str, Any] | None:
        """记录连接器在返回确认前退出：接收事实保留，仅标记确认未送达调用方。"""
        with self._lock:
            receipt = self.get_by_receipt_no(receipt_no)
            if receipt is None:
                return None
            receipt["确认送达"] = False
            return receipt

    # ---- 消息流源：凭接收确认选择可消费输入 ----

    def consumable_inputs(
        self,
        *,
        source_key: str | None = None,
        topic: str | None = None,
    ) -> list[dict[str, Any]]:
        """只从已签收且未消费的凭据中选输入，并对应原交付编号与收到的内容。"""
        rows = self._rows()
        if source_key is not None:
            rows = [row for row in rows if row.get("来源标识") == source_key]
        if topic is not None:
            rows = [row for row in rows if row.get("Topic") == topic]
        return [
            {
                "签收凭据": row["签收凭据"],
                "来源标识": row["来源标识"],
                "Topic": row["Topic"],
                "原交付编号": row["交付编号"],
                "收到的内容": row["原始内容"],
                "内容指纹": row["内容指纹"],
                "首次接收时间": row["首次接收时间"],
            }
            for row in rows
            if not row.get("已消费")
        ]

    def consume(self, receipt_no: str) -> dict[str, Any]:
        """按签收凭据实际消费载荷数据，同一凭据不可重复消费。"""
        with self._lock:
            receipt = self.get_by_receipt_no(receipt_no)
            if receipt is None:
                return {"ok": False, "message": f"签收凭据 {receipt_no} 不存在"}
            if receipt.get("已消费"):
                return {
                    "ok": False,
                    "message": "该输入已消费，拒绝重复消费",
                    "receipt": receipt,
                }
            receipt["已消费"] = True
            receipt["消费时间"] = _now()
            receipt["status"] = "已消费"
            return {
                "ok": True,
                "message": "载荷已实际消费",
                "receipt_no": receipt_no,
                "delivery_id": receipt["交付编号"],
                "message_id": receipt["载荷消息ID"],
                "payload": receipt["原始内容"],
            }

    def valid_delivery_count(self, source_key: str) -> int:
        """有效交付数 = 台账中该来源的签收据条数（冲突/重送不产生新凭据）。"""
        return sum(1 for row in self._rows() if row.get("来源标识") == source_key)


# 进程内单例：连接器、消息流源、路由共用同一份台账与同一把裁决锁。
delivery_ledger = DeliveryLedgerService()
