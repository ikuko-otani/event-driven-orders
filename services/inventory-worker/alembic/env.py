"""Alembic environment for the `inventory` schema — inventory-worker's own history.

Two settings make this history schema-scoped instead of database-scoped, which
is what keeps the service ownership of design §3.1 enforced by the tooling:

  * version_table_schema — alembic_version lives inside `inventory`, so the two
    services' histories are independent and cannot overwrite each other.
  * include_object — autogenerate is shown only objects in `inventory`, so a table
    owned by order-api is never mistaken for an extra table and dropped.
"""

from typing import Any

from alembic import context
from sqlalchemy import create_engine, text

from common.settings import DatabaseSettings
from inventory_worker.models import Base
from inventory_worker.models.base import SCHEMA

target_metadata = Base.metadata


def include_object(
    obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any
) -> bool:
    """Keep autogenerate inside this service's own schema (design §3.1)."""
    if type_ == "table":
        return bool(obj.schema == SCHEMA)
    return True


def run_migrations_online() -> None:
    engine = create_engine(DatabaseSettings().sync_url)
    with engine.connect() as connection:
        # The schema must exist before Alembic can put alembic_version in it.
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=SCHEMA,
            include_schemas=True,
            include_object=include_object,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    raise RuntimeError("Offline mode is unsupported: env.py connects to the database.")

run_migrations_online()
