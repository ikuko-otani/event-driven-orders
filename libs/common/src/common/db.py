"""Shared SQLAlchemy plumbing: one MetaData per owning schema.

Each service owns exactly one schema (design §3.1), so each builds its own
DeclarativeBase on a MetaData bound to that schema. Nothing here opens a
connection: the two services never share a session, only this code.
"""

from sqlalchemy import MetaData

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
