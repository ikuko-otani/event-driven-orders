"""The FastAPI app is reachable end-to-end, with its own lifespan running."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_returns_ok(api_client: AsyncClient) -> None:
    response = await api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
