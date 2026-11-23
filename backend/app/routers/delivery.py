"""幂等交付接收与凭据消费接口。

把 API 连接器的幂等接收、凭据查询、确认丢失模拟，以及消息流源的可消费输入
选择、实际消费、恢复重放收敛到一组固定路径上，避免与通用资源路由的
``/api/{module}`` 动态段冲突。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.api_connector import ApiConnectorService
from app.services.delivery import delivery_ledger
from app.services.stream_source import StreamSourceService

router = APIRouter(prefix="/api/delivery", tags=["幂等交付"])

connector_service = ApiConnectorService()
stream_service = StreamSourceService()


class IngestPayload(BaseModel):
    delivery_id: str = Field(..., description="上游交付编号，幂等键的一部分")
    payload: dict[str, Any] = Field(default_factory=dict, description="交付载荷数据")


class LookupPayload(BaseModel):
    delivery_id: str


class RecoverPayload(BaseModel):
    source_key: str = Field(..., description="连接器来源标识，如 api-connector:1")
    delivery_id: str
    payload: dict[str, Any] = Field(default_factory=dict)


@router.post("/connectors/{entry_id}/ingest")
def ingest_delivery(entry_id: int, body: IngestPayload) -> dict[str, Any]:
    """连接器接收一次上游交付（首次接收与重送共用此入口）。"""
    return connector_service.ingest_delivery(entry_id, body.delivery_id, body.payload)


@router.post("/connectors/{entry_id}/lookup")
def lookup_delivery(entry_id: int, body: LookupPayload) -> dict[str, Any]:
    """调用方查询交付结果，取回首张签收凭据。"""
    return connector_service.lookup_delivery(entry_id, body.delivery_id)


@router.post("/connectors/{entry_id}/simulate-ack-lost")
def simulate_ack_lost(entry_id: int, body: LookupPayload) -> dict[str, Any]:
    """模拟载荷已入库但确认在返回前丢失。"""
    return connector_service.simulate_ack_lost(entry_id, body.delivery_id)


@router.get("/receipts")
def list_receipts(source_key: str | None = None) -> dict[str, Any]:
    """列出交付签收凭据，可按来源标识过滤。"""
    rows = delivery_ledger.list_receipts(source_key)
    return {"items": rows, "total": len(rows)}


@router.post("/streams/{entry_id}/consumable")
def consumable_inputs(entry_id: int) -> dict[str, Any]:
    """消息流源凭接收确认选定实际可消费的输入。"""
    return stream_service.consumable_inputs(entry_id)


@router.post("/streams/{entry_id}/consume/{receipt_no}")
def consume_confirmed(entry_id: int, receipt_no: str) -> dict[str, Any]:
    """按签收凭据实际消费载荷数据。"""
    return stream_service.consume_confirmed(entry_id, receipt_no)


@router.post("/streams/recover")
def recover_pending_delivery(body: RecoverPayload) -> dict[str, Any]:
    """恢复入口重放待确认交付，与首收共用同一裁决逻辑。"""
    return stream_service.recover_pending_delivery(
        body.source_key, body.delivery_id, body.payload)
