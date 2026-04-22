from pydantic import BaseModel, ConfigDict, field_validator
from datetime import datetime
from typing import List, Optional, Literal
from decimal import Decimal

from .models import EventType


# ── Inbound ──────────────────────────────────────────────────────────────────

class EventCreate(BaseModel):
    event_id: str
    event_type: EventType          # validated against the four known lifecycle values
    transaction_id: str
    merchant_id: str
    merchant_name: str
    amount: Decimal
    currency: str
    timestamp: datetime


# ── Outbound ─────────────────────────────────────────────────────────────────

class MerchantResponse(BaseModel):
    merchant_id: str
    merchant_name: str

    model_config = ConfigDict(from_attributes=True)


class EventResponse(BaseModel):
    event_id: str
    event_type: EventType
    timestamp: datetime

    model_config = ConfigDict(from_attributes=True)


class TransactionResponse(BaseModel):
    transaction_id: str
    merchant_id: str
    merchant: Optional[MerchantResponse] = None   # populated on detail endpoint
    amount: Decimal
    currency: str
    current_status: EventType
    latest_event_timestamp: datetime
    created_at: Optional[datetime] = None
    events: List[EventResponse] = []

    model_config = ConfigDict(from_attributes=True)


# ── Query params (kept as a schema for reuse / docs) ──────────────────────────

SortField = Literal["created_at", "latest_event_timestamp", "amount"]
SortDir = Literal["asc", "desc"]
