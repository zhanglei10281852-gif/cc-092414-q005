from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class TraceEntry(BaseModel):
    lot_code: str = Field(..., min_length=3, max_length=64)
    quantity_kg: float = Field(..., gt=0, le=10_000_000)

    @field_validator("lot_code")
    @classmethod
    def normalize(cls, value: str) -> str:
        return value.strip().upper()


class OriginEventCreate(BaseModel):
    lot_code: str = Field(..., min_length=3, max_length=64)
    quantity_kg: float = Field(..., gt=0, le=10_000_000)
    event_time: str = Field(..., min_length=10, max_length=40)
    supplier: str = Field(..., min_length=1, max_length=120)
    origin: str = Field(..., min_length=1, max_length=160)
    product_name: str = Field(default="", max_length=120)
    harvest_date: str = Field(default="", max_length=40)
    certificate_no: str = Field(default="", max_length=80)
    documents: list[str] = Field(default_factory=list)
    actor: str = Field(default="", max_length=80)

    @field_validator("lot_code")
    @classmethod
    def normalize(cls, value: str) -> str:
        return value.strip().upper()


class SplitEventCreate(BaseModel):
    lot_code: str = Field(..., min_length=3, max_length=64)
    quantity_kg: float = Field(..., gt=0, le=10_000_000)
    event_time: str = Field(..., min_length=10, max_length=40)
    outputs: list[TraceEntry] = Field(..., min_length=2)
    note: str = Field(default="", max_length=300)
    documents: list[str] = Field(default_factory=list)
    actor: str = Field(default="", max_length=80)

    @field_validator("lot_code")
    @classmethod
    def normalize(cls, value: str) -> str:
        return value.strip().upper()


class MergeEventCreate(BaseModel):
    lot_code: str = Field(..., min_length=3, max_length=64)
    quantity_kg: float = Field(..., gt=0, le=10_000_000)
    event_time: str = Field(..., min_length=10, max_length=40)
    inputs: list[TraceEntry] = Field(..., min_length=2)
    note: str = Field(default="", max_length=300)
    documents: list[str] = Field(default_factory=list)
    actor: str = Field(default="", max_length=80)

    @field_validator("lot_code")
    @classmethod
    def normalize(cls, value: str) -> str:
        return value.strip().upper()


class HandoverEventCreate(BaseModel):
    lot_code: str = Field(..., min_length=3, max_length=64)
    quantity_kg: float = Field(..., gt=0, le=10_000_000)
    event_time: str = Field(..., min_length=10, max_length=40)
    from_node: str = Field(..., min_length=1, max_length=160)
    to_node: str = Field(..., min_length=1, max_length=160)
    handler: str = Field(default="", max_length=80)
    shipment_code: str = Field(default="", max_length=64)
    documents: list[str] = Field(default_factory=list)
    actor: str = Field(default="", max_length=80)

    @field_validator("lot_code")
    @classmethod
    def normalize(cls, value: str) -> str:
        return value.strip().upper()


class RevocationCreate(BaseModel):
    reason: str = Field(..., min_length=1, max_length=300)
    actor: str = Field(default="supervisor", min_length=1, max_length=80)
    event_time: str = Field(default="", max_length=40)
