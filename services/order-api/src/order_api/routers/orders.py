"""HTTP routes for order creation."""

import uuid
from typing import Any

from fastapi import APIRouter, Depends, Header, Response, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.cache import get_cached_response, get_redis, set_cached_response
from order_api.db import get_session
from order_api.models import Order
from order_api.schemas.orders import OrderCreate, OrderRead
from order_api.services.orders import create_order, fingerprint, get_order

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", response_model=OrderRead, status_code=status.HTTP_201_CREATED)
async def post_order(
    body: OrderCreate,
    response: Response,
    x_entity_id: uuid.UUID = Header(...),
    idempotency_key: str = Header(...),
    session: AsyncSession = Depends(get_session),
    redis: Redis = Depends(get_redis),
) -> Order | dict[str, Any]:
    request_fingerprint = fingerprint(body)
    cached = await get_cached_response(
        redis, entity_id=x_entity_id, idempotency_key=idempotency_key
    )
    if cached is not None and cached["request_fingerprint"] == request_fingerprint:
        response.status_code = status.HTTP_200_OK
        cached_order: dict[str, Any] = cached["order"]
        return cached_order

    order, created = await create_order(
        session,
        entity_id=x_entity_id,
        idempotency_key=idempotency_key,
        body=body,
    )

    if not created:
        response.status_code = status.HTTP_200_OK
    await set_cached_response(
        redis,
        entity_id=x_entity_id,
        idempotency_key=idempotency_key,
        body={
            "request_fingerprint": request_fingerprint,
            "order": OrderRead.model_validate(order).model_dump(mode="json"),
        },
    )
    return order


@router.get("/{order_id}", response_model=OrderRead)
async def get_order_by_id(
    order_id: uuid.UUID,
    x_entity_id: uuid.UUID = Header(...),
    session: AsyncSession = Depends(get_session),
) -> Order:
    return await get_order(session, entity_id=x_entity_id, order_id=order_id)
