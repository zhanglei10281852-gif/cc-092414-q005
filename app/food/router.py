from __future__ import annotations

import sqlite3

from fastapi import APIRouter, HTTPException, Query, Response

from app.food.chain import TraceChainService
from app.food.schemas import LotCreate, RiskDecision, SampleCreate, ShipmentCreate, TemperatureRecord, TestResultCreate, TraceEventCreate
from app.food.service import FoodService

router = APIRouter(prefix="/api/food", tags=["食品安全"])


def service() -> FoodService:
    return FoodService()


def chain_service() -> TraceChainService:
    return TraceChainService()


@router.post("/lots", status_code=201)
def create_lot(payload: LotCreate):
    try:
        return service().create_lot(payload.model_dump(), actor=payload.supplier)
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=409, detail="批次编码或追溯码已存在") from exc
        raise


@router.get("/lots/{lot_id}")
def get_lot(lot_id: int, details: bool = True):
    value = service().get_lot(lot_id, details)
    if value is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    return value


@router.get("/lots/{lot_id}/summary")
def summary(lot_id: int):
    try:
        return service().summary(lot_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="批次不存在") from exc


@router.delete("/lots/{lot_id}")
def delete_lot(lot_id: int):
    try:
        service().delete_lot(lot_id)
        return {"message": "批次已删除"}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="批次不存在") from exc


@router.post("/lots/{lot_id}/samples", status_code=201)
def add_sample(lot_id: int, payload: SampleCreate):
    try:
        return service().add_sample(lot_id, payload.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="批次不存在") from exc


@router.post("/samples/{sample_id}/results", status_code=201)
def add_result(sample_id: int, payload: TestResultCreate):
    try:
        return service().add_result(sample_id, payload.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="样品不存在") from exc


@router.post("/lots/{lot_id}/shipments", status_code=201)
def create_shipment(lot_id: int, payload: ShipmentCreate):
    try:
        return service().create_shipment(lot_id, payload.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="批次不存在") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/shipments/{shipment_id}/temperatures", status_code=201)
def add_temperature(shipment_id: int, payload: TemperatureRecord):
    try:
        return service().add_temperature(shipment_id, payload.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="运输单不存在") from exc


@router.post("/lots/{lot_id}/risk", status_code=200)
def decide_risk(lot_id: int, payload: RiskDecision):
    try:
        return service().decide_risk(lot_id, payload.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="批次不存在") from exc


@router.post("/trace/events")
def append_trace_event(payload: TraceEventCreate, response: Response):
    try:
        event, replayed = chain_service().append_event(payload.model_dump())
    except KeyError as exc:
        missing = exc.args[0] if exc.args else ""
        raise HTTPException(status_code=404, detail=f"批次或目标事件不存在: {missing}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail="批次编码或追溯码已存在") from exc
    response.status_code = 200 if replayed else 201
    return event


@router.get("/trace/events")
def list_trace_events(
    event_type: str | None = Query(None),
    lot_code: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    events = chain_service().list_events(
        event_type=event_type,
        lot_code=lot_code.strip().upper() if lot_code else None,
        limit=limit,
        offset=offset,
    )
    return {"events": events}


@router.get("/trace/events/{event_id}")
def get_trace_event(event_id: int):
    event = chain_service().get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="追溯事件不存在")
    return event


@router.get("/trace/verify")
def verify_trace_chain():
    return chain_service().verify_chain()


@router.get("/lots/{lot_id}/balance")
def lot_balance(lot_id: int):
    try:
        return chain_service().lot_balance(lot_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="批次不存在") from exc


@router.get("/lots/{lot_id}/trace/upstream")
def trace_upstream(lot_id: int):
    try:
        return chain_service().trace_upstream(lot_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="批次不存在") from exc


@router.get("/lots/{lot_id}/trace/downstream")
def trace_downstream(lot_id: int):
    try:
        return chain_service().trace_downstream(lot_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="批次不存在") from exc
