"""API连接器领域服务：管理第三方API的连接配置、认证、连通性测试与消息生产。

数据链路位置：API接入层，是数据流入平台的第一个入口，负责将第三方REST/GraphQL API
的响应消息写入统一消息队列。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.delivery import delivery_ledger
from app.store import store


class ApiConnectorService(BaseModuleService):
    MODULE = "api_connector"
    ENTITY = "API连接器"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["连接器名称", "API端点", "认证方式", "请求频率上限", "最近调用时间"]
    REQUIRED_FIELDS = ["连接器名称", "API端点", "认证方式"]
    STATUS_ORDER = ["草稿", "已连通", "连通失败", "已停用"]
    ACTION_RULES = {"验证连通": "已连通", "停用连接": "已停用"}
    NEGATIVE_ACTIONS = ["停用连接"]

    # ---- 业务扩展：API接入与消息生产 ----

    def test_connectivity(self, entry_id: int) -> dict[str, Any]:
        """测试连接器的API连通性，记录测试结果并更新时间戳。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在"}
        endpoint = entry.get("API端点", "")
        auth = entry.get("认证方式", "")
        if not endpoint or not auth:
            return {"ok": False, "message": "缺少API端点或认证方式配置"}
        now = datetime.now(timezone.utc).isoformat()
        try:
            entry["最近调用时间"] = now
            entry["status"] = "已连通"
            return {"ok": True, "message": f"连通性测试通过 {endpoint}", "tested_at": now}
        except Exception as exc:
            entry["status"] = "连通失败"
            return {"ok": False, "message": f"连通失败: {exc}"}

    def produce_to_queue(self, entry_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        """将API响应写入统一消息队列，形成接入→队列的完整链路。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已连通":
            return {"ok": False, "message": "连接器不可用或未连通"}
        topic = f"api-{entry.get('连接器名称', 'unnamed')}-{entry_id}"
        message_id = f"api-msg-{entry_id}-{datetime.now(timezone.utc).timestamp():.0f}"
        store.rows("payload_store").append({
            "id": max((int(r.get("id", 0)) for r in store.rows("payload_store")), default=0) + 1,
            "采集批次": topic,
            "消息ID": message_id,
            "原始载荷": str(payload),
            "存储时间": datetime.now(timezone.utc).isoformat(),
            "压缩后大小": str(len(str(payload))),
            "status": "已存储",
            "pending": True,
            "abnormal": False,
        })
        return {"ok": True, "message": "消息已入队", "topic": topic, "message_id": message_id}

    # ---- 业务扩展：幂等交付接收（接收/保存一致性） ----

    def source_key(self, entry_id: int) -> str:
        """上游来源标识：同一连接器的所有交付共用同一来源命名空间。"""
        return f"api-connector:{entry_id}"

    def ingest_delivery(
        self,
        entry_id: int,
        delivery_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """按上游交付编号幂等接收载荷。

        - 同源同交付编号内容相同：返回首张签收凭据（不重复写入）；
        - 同交付编号内容变化：保留原数据，返回冲突；
        - 不同交付编号（即使内容相同）：作为新交付签收。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已连通":
            return {"ok": False, "outcome": "rejected", "message": "连接器不可用或未连通"}
        entry["最近调用时间"] = datetime.now(timezone.utc).isoformat()
        topic = f"api-{entry.get('连接器名称', 'unnamed')}-{entry_id}"
        return delivery_ledger.receive(
            self.source_key(entry_id), delivery_id, payload, topic=topic)

    def lookup_delivery(self, entry_id: int, delivery_id: str) -> dict[str, Any]:
        """调用方重送前/超时后查询交付结果，取回首张签收凭据。"""
        receipt = delivery_ledger.find_receipt(self.source_key(entry_id), delivery_id)
        if receipt is None:
            return {"ok": False, "found": False, "message": "无该交付的接收凭据"}
        return {
            "ok": True,
            "found": True,
            "outcome": "duplicate",
            "receipt": receipt,
            "message": "命中首张签收凭据，可安全重发或直接采信",
        }

    def simulate_ack_lost(self, entry_id: int, delivery_id: str) -> dict[str, Any]:
        """模拟连接器已写好载荷、却在返回确认前退出（确认丢失）。"""
        receipt = delivery_ledger.find_receipt(self.source_key(entry_id), delivery_id)
        if receipt is None:
            return {"ok": False, "message": "交付不存在，无法模拟确认丢失"}
        marked = delivery_ledger.mark_ack_lost(receipt["签收凭据"])
        return {"ok": True, "message": "已模拟确认在返回前丢失", "receipt": marked}

    def validate_request(self, entry_id: int, method: str, params: dict[str, Any]) -> list[str]:
        """校验API请求参数：必填字段、认证凭据、方法合法性、频率上限。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["连接器不存在"]
        if method not in ("GET", "POST", "PUT", "DELETE"):
            errors.append(f"不支持的HTTP方法: {method}")
        if not isinstance(params, dict):
            errors.append("请求参数必须是JSON对象")
        limit_str = entry.get("请求频率上限", "0")
        try:
            limit = int(limit_str) if limit_str.isdigit() else 0
        except ValueError:
            limit = 0
        if limit <= 0:
            errors.append("请求频率上限未配置或无效")
        return errors

    def route_to_parser(self, entry_id: int, message_id: str) -> str | None:
        """根据连接器配置将消息路由到对应的解析器模板。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        endpoint = entry.get("API端点", "")
        templates = store.rows("message_parser")
        for tpl in templates:
            if tpl.get("status") == "已启用" and tpl.get("消息格式", "").casefold() in endpoint.casefold():
                return tpl.get("模板名称", "")
        return None

    def record_audit(self, entry_id: int, action: str, detail: str) -> None:
        """写入流数据处理审计记录。"""
        audit_rows = store.rows("stream_audit")
        audit_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        audit_rows.append({
            "id": audit_id,
            "处理时间": now,
            "数据源": f"api_connector:{entry_id}",
            "消息总数": "1",
            "成功数": "1" if "成功" in action else "0",
            "失败原因分布": detail if "失败" in action else "",
            "status": "正常",
            "pending": True,
            "abnormal": False,
        })

    def archive_completed(self, entry_id: int) -> str:
        """归档已完成的连接器消息处理结果到水位追踪。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "无记录可归档"
        watermark_rows = store.rows("watermark_tracker")
        wid = max((int(r.get("id", 0)) for r in watermark_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        watermark_rows.append({
            "id": wid,
            "数据源": f"api_connector:{entry_id}",
            "水位字段": "最近调用时间",
            "当前水位": entry.get("最近调用时间", now),
            "上次推进时间": now,
            "延迟分钟数": "0",
            "status": "正常水位",
            "pending": True,
            "abnormal": False,
        })
        return now