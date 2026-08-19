from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from order_api.db import make_engine, make_sessionmaker
from order_api.routers.orders import router as order_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    engine = make_engine()
    app.state.sessionmaker = make_sessionmaker(engine)
    yield
    await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(order_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
