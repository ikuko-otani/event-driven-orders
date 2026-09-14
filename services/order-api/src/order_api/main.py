from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from common.observability import adopt_loggers, configure
from order_api.cache import make_redis_client
from order_api.db import make_engine, make_sessionmaker
from order_api.routers.orders import router as orders_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Before anything else: a line written before the pipeline exists comes out
    # in whatever format was in place, in the middle of a JSON stream.
    configure("order-api")

    # uvicorn installs handlers of its own before a lifespan ever runs, and its
    # loggers do not propagate, so the access log would stay outside the
    # pipeline (§6.6). Its two lines before this point are text, and stay text.
    adopt_loggers("uvicorn", "uvicorn.access", "uvicorn.error")

    engine = make_engine()
    app.state.sessionmaker = make_sessionmaker(engine)
    app.state.redis = make_redis_client()
    yield
    await app.state.redis.aclose()
    await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(orders_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
