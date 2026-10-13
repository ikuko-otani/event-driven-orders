"""Shared SQLAlchemy plumbing: schema-scoped MetaData and engine factories.

Each service owns exactly one schema (design §3.1), so each builds its own
DeclarativeBase on a MetaData bound to that schema. The engine factories are
shared so that how a pooled connection is checked is decided once for every
process; building an engine opens no connection, and no two services share one.
"""

from sqlalchemy import Engine, MetaData, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from common.settings import DatabaseSettings

# Deterministic constraint names. Without a convention PostgreSQL invents
# names for unnamed constraints, and a later Alembic migration cannot drop or
# alter what it cannot name.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def make_metadata(schema: str) -> MetaData:
    """Build a MetaData scoped to one schema, with stable constraint names."""
    return MetaData(schema=schema, naming_convention=NAMING_CONVENTION)


def make_sync_engine() -> Engine:
    """Build the engine a poller or consumer works through, from the environment.

    pool_pre_ping tests a pooled connection before handing it out, so a
    database restart costs one reconnect instead of a failed unit of work.
    """
    return create_engine(DatabaseSettings().sync_url, pool_pre_ping=True)


def make_async_engine() -> AsyncEngine:
    """Build the HTTP service's engine, checked the same way as make_sync_engine."""
    return create_async_engine(DatabaseSettings().async_url, pool_pre_ping=True)
