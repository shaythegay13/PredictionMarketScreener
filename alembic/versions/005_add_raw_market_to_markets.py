"""Add raw_market to markets table.

Revision ID: 005_add_raw_market_to_markets
Revises: 004_add_depth_fetched
Create Date: 2026-07-25

Adds raw_market JSONB column to the markets table to store the latest raw shape of the market.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "005_add_raw_market_to_markets"
down_revision: Union[str, None] = "004_add_depth_fetched"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "markets",
        sa.Column("raw_market", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("markets", "raw_market")
