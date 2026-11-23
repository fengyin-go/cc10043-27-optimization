"""幂等交付一致性模拟：确认丢失后重送/冲突/异号同内容的完整演练。

场景
----
1. 订单 x（交付编号 order-x）经连接器首次接收入库；
2. 模拟连接器在返回确认前退出——调用方没收到响应，消费端可能已看到输入；
3. 发送方依次：
   a. 原样重送 x；
   b. 用同一交付编号送内容已变的新内容；
   c. 用另一个交付编号 order-y 送与 x 完全相同的内容；
4. 消息流源按签收凭据选择可消费输入并实际消费；
5. 核对最终只有两个有效交付，且两个入口对同一交付判断一致。

运行（在 backend/ 目录下）：
    .venv/bin/python -I sim/idempotent_delivery_demo.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# -I 隔离模式下不会自动带上当前目录，显式注入 backend 根以导入 app 包。
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.api_connector import ApiConnectorService  # noqa: E402
from app.services.delivery import delivery_ledger  # noqa: E402
from app.services.stream_source import StreamSourceService  # noqa: E402

failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {label}" + (f" -> {detail}" if detail else ""))
    if not condition:
        failures.append(label)


def main() -> int:
    connector = ApiConnectorService()
    stream = StreamSourceService()

    # ---- 准备：一个已连通的连接器 + 一个运行中、Topic 对齐的消息流源 ----
    conn_entry, missing = connector.create_entry({
        "连接器名称": "订单回调连接器",
        "API端点": "https://upstream.example.com/orders",
        "认证方式": "BearerToken",
        "请求频率上限": "100",
    })
    connector_id = conn_entry["id"]
    connector.run_action(connector_id, "验证连通")
    topic = f"api-订单回调连接器-{connector_id}"
    source_key = connector.source_key(connector_id)

    stream_entry, _ = stream.create_entry({
        "源名称": "订单回调消息流",
        "中间件类型": "Kafka",
        "Topic/Queue名": topic,
        "消费组": "order-cg",
        "偏移量策略": "earliest",
    })
    stream_id = stream_entry["id"]
    stream.run_action(stream_id, "启动消费")
    print(f"准备完成: connector={connector_id} source_key={source_key} topic={topic}\n")

    order_x = {"order_id": "x", "amount": 100, "status": "CREATED"}
    order_x_changed = {"order_id": "x", "amount": 250, "status": "PAID"}

    # ---- 1. 订单 x 首次接收入库 ----
    first = connector.ingest_delivery(connector_id, "order-x", order_x)
    check("首次接收 order-x 被签收", first["outcome"] == "accepted", first["outcome"])
    first_receipt = first["receipt"]
    first_receipt_no = first_receipt["签收凭据"]
    check("首张凭据编号已生成", bool(first_receipt_no), first_receipt_no)

    # ---- 2. 连接器在返回确认前退出：载荷已入库，确认丢失 ----
    lost = connector.simulate_ack_lost(connector_id, "order-x")
    check("确认丢失被标记但接收事实保留",
          lost["ok"] and lost["receipt"]["确认送达"] is False)

    # 消费端此时已经能从凭据里看到这条输入（确认丢失不影响已落库的接收）。
    seen = stream.consumable_inputs(stream_id)
    check("确认丢失后消费端仍能看到该输入",
          seen["count"] == 1 and seen["inputs"][0]["原交付编号"] == "order-x")

    # ---- 3a. 发送方原样重送 x：必须取回同一张凭据，不重复写入 ----
    resend = connector.ingest_delivery(connector_id, "order-x", order_x)
    check("原样重送判定 duplicate", resend["outcome"] == "duplicate", resend["outcome"])
    check("重送取回的是首张签收凭据",
          resend["receipt"]["签收凭据"] == first_receipt_no,
          resend["receipt"]["签收凭据"])

    # 调用方也可以显式查询重送结果。
    lookup = connector.lookup_delivery(connector_id, "order-x")
    check("查询接口取回首张凭据",
          lookup["found"] and lookup["receipt"]["签收凭据"] == first_receipt_no)

    # 恢复入口（消息流侧重放）对同一交付必须给出一致判断。
    recovered = stream.recover_pending_delivery(source_key, "order-x", order_x)
    check("恢复入口对原内容同样判定 duplicate 并返回同凭据",
          recovered["outcome"] == "duplicate"
          and recovered["receipt"]["签收凭据"] == first_receipt_no,
          recovered["outcome"])

    # ---- 3b. 同交付编号送新内容：保留原数据并告知冲突 ----
    conflict = connector.ingest_delivery(connector_id, "order-x", order_x_changed)
    check("同号异内容判定 conflict", conflict["outcome"] == "conflict", conflict["outcome"])
    stored = connector.lookup_delivery(connector_id, "order-x")["receipt"]
    check("冲突后保留的仍是首次接收的原始内容",
          stored["原始内容"] == first_receipt["原始内容"]
          and stored["冲突次数"] == 1
          and stored["签收凭据"] == first_receipt_no)

    # 恢复入口对冲突内容的判断也必须与首收入口一致。
    recovered_conflict = stream.recover_pending_delivery(
        source_key, "order-x", order_x_changed)
    check("恢复入口对同号异内容同样判定 conflict",
          recovered_conflict["outcome"] == "conflict")
    check("两个入口冲突判断一致（冲突次数共享）",
          stored["冲突次数"] == 2)

    # ---- 3c. 另一个交付编号、内容碰巧相同：仍是新交付 ----
    another = connector.ingest_delivery(connector_id, "order-y", order_x)
    check("异号同内容判定 accepted（新交付）",
          another["outcome"] == "accepted"
          and another["receipt"]["签收凭据"] != first_receipt_no,
          another["outcome"])
    second_receipt_no = another["receipt"]["签收凭据"]

    # ---- 4. 消息流源凭接收确认选择可消费输入并实际消费 ----
    pending = stream.consumable_inputs(stream_id)
    check("可消费输入恰好两条（order-x / order-y）",
          pending["count"] == 2
          and {item["原交付编号"] for item in pending["inputs"]} == {"order-x", "order-y"})
    sample = pending["inputs"][0]
    check("可消费输入已对应原交付编号与收到的内容",
          sample["原交付编号"] == "order-x"
          and sample["签收凭据"] == first_receipt_no
          and '"order_id":"x"' in sample["收到的内容"])

    consumed_x = stream.consume_confirmed(stream_id, first_receipt_no)
    check("order-x 载荷被实际消费",
          consumed_x["ok"] and consumed_x["delivery_id"] == "order-x"
          and '"amount":100' in consumed_x["payload"],
          consumed_x.get("message", ""))
    consumed_y = stream.consume_confirmed(stream_id, second_receipt_no)
    check("order-y 载荷被实际消费", consumed_y["ok"], consumed_y.get("message", ""))
    double_consume = stream.consume_confirmed(stream_id, first_receipt_no)
    check("同一凭据不可重复消费", not double_consume["ok"], double_consume["message"])

    # ---- 5. 最终核对：只有两个有效交付，载荷表也只有两条 ----
    valid = delivery_ledger.valid_delivery_count(source_key)
    check("台账中有效交付数为 2", valid == 2, f"actual={valid}")
    receipts = delivery_ledger.list_receipts(source_key)
    check("签收凭据恰好两张（重送/冲突均未新增）", len(receipts) == 2)
    check("消费后无残留可消费输入",
          len(delivery_ledger.consumable_inputs(source_key=source_key)) == 0)

    print("\n==== 交付凭据明细 ====")
    for row in delivery_ledger.list_receipts(source_key):
        print(
            f"凭据={row['签收凭据']} 交付编号={row['交付编号']} "
            f"状态={row['status']} 重发={row['重发次数']} 冲突={row['冲突次数']} "
            f"确认送达={row['确认送达']} 已消费={row['已消费']}"
        )

    print("\n==== 结果汇总 ====")
    if failures:
        print(f"模拟失败，{len(failures)} 项断言未通过: {failures}")
        return 1
    print("全部断言通过：确认丢失后重送去重、冲突保留原数据、异号同内容独立签收，"
          "最终仅两个有效交付且均已消费。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
