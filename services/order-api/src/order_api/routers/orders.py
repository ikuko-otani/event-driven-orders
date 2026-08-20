"""HTTP routes for order creation."""

import uuid

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.db import get_session
from order_api.models import Order
from order_api.schemas.orders import OrderCreate, OrderRead
from order_api.services.orders import create_order

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", response_model=OrderRead, status_code=status.HTTP_201_CREATED)
async def post_order(
    body: OrderCreate,
    x_entity_id: uuid.UUID = Header(...),
    idempotency_key: str = Header(...),
    session: AsyncSession = Depends(get_session),
) -> Order:
    return await create_order(
        session,
        entity_id=x_entity_id,
        idempotency_key=idempotency_key,
        body=body,
    )
