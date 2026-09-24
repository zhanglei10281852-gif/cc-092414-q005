from __future__ import annotations

from fastapi import APIRouter, Query

from app.core.errors import NotFoundError, ValidationError
from app.trace.schemas import HandoverEventCreate, MergeEventCreate, OriginEventCreate, RevocationCreate, SplitEventCreate
from app.trace.service import TraceService

router = APIRouter(prefix="/api/trace", tags=["食品追溯事件链"])


def service() -> TraceService:
    return TraceService()


@router.post("/origins", status_code=201)
def record_origin(payload: OriginEventCreate):
    return service().record_origin(payload.model_dump())


@router.post("/splits", status_code=201)
def record_split(payload: SplitEventCreate):
    data = payload.model_dump()
    return service().record_split(data)


@router.post("/merges", status_code=201)
def record_merge(payload: MergeEventCreate):
    return service().record_merge(payload.model_dump())


@router.post("/handovers", status_code=201)
def record_handover(payload: HandoverEventCreate):
    return service().record_handover(payload.model_dump())


@router.post("/events/{event_id}/revoke", status_code=201)
def revoke_event(event_id: int, payload: RevocationCreate):
    data = payload.model_dump()
    event_time = data.pop("event_time") or None
    return service().revoke_event(event_id, data["reason"], actor=data["actor"], event_time=event_time)


@router.get("/events/{event_id}")
def get_event(event_id: int):
    value = service().get_event(event_id)
    if value is None:
        raise NotFoundError("追溯事件不存在", context={"event_id": event_id})
    return value


@router.get("/lots/{lot_code}/history")
def lot_history(lot_code: str):
    return service().history(lot_code.strip().upper())


@router.get("/lots/{lot_code}/lineage")
def lot_lineage(lot_code: str, direction: str = Query("both")):
    if direction not in {"upstream", "downstream", "both"}:
        raise ValidationError("方向只能是 upstream、downstream 或 both", context={"direction": direction})
    return service().lineage(lot_code.strip().upper(), direction)


@router.get("/verify")
def verify_chain():
    return service().verify_chain()
