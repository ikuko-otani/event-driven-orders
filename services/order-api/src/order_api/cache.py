"""Idempotency-Key response cache in front of the DB constraint (design §4.4)."""

import json
import uuid
from typing import Any

from fastapi import Request
from redis.asyncio import Redis

from common.settings import RedisSettings

TTL_SECONDS = 24 * 60 * 60


def make_redis_client() -> Redis:
    return Redis.from_url(RedisSettings().url)


def get_redis(request: Request) -> Redis:
    redis: Redis = request.app.state.redis
    return redis


def _cache_key(*, entity_id: uuid.UUID, idempotency_key: str) -> str:
    return f"idempotency:{entity_id}:{idempotency_key}"


async def get_cached_response(
    redis: Redis, *, entity_id: uuid.UUID, idempotency_key: str
) -> dict[str, Any] | None:
    cached = await redis.get(_cache_key(entity_id=entity_id, idempotency_key=idempotency_key))
    if cached is None:
        return None
    result: dict[str, Any] = json.loads(cached)
    return result


async def set_cached_response(
    redis: Redis, *, entity_id: uuid.UUID, idempotency_key: str, body: dict[str, Any]
) -> None:
    await redis.set(
        _cache_key(entity_id=entity_id, idempotency_key=idempotency_key),
        json.dumps(body),
        ex=TTL_SECONDS,
    )
