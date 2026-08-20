"""Pydantic request/response models for the order-create endpoint."""

import uuid
from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, Field

MAX_LINES = 50


class OrderLineCreate(BaseModel):
    item_id: uuid.UUID
    quantity: int = Field(gt=0)


class OrderCreate(BaseModel):
    customer_id: uuid.UUID
    currency: str = Field(min_length=3, max_length=3)
    delivery_date: date
    lines: list[OrderLineCreate] = Field(min_length=1, max_length=MAX_LINES)


class OrderLineRead(BaseModel):
    item_id: uuid.UUID
    quantity: int
    unit_price: Decimal

    model_config = {"from_attributes": True}


class OrderRead(BaseModel):
    id: uuid.UUID
    entity_id: uuid.UUID
    customer_id: uuid.UUID
    order_number: str
    status: str
    currency: str
    delivery_date: date
    created_at: datetime
    lines: list[OrderLineRead]

    model_config = {"from_attributes": True}
