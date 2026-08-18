"""create order number sequence

Revision ID: 08cbd923c71c
Revises: e9c79f38ebe1
Create Date: 2026-08-18 17:41:41.026761

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "08cbd923c71c"
down_revision: str | Sequence[str] | None = "e9c79f38ebe1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE SEQUENCE orders.order_number_seq")


def downgrade() -> None:
    op.execute("DROP SEQUENCE orders.order_number_seq")
