"""Add depth_fetched column to market_snapshots.

Revision ID: 004_add_depth_fetched
Revises: 003_add_spread_column
Create Date: 2026-07-25

depth_fetched = false  → tier-1 row: quotes from /events nested market object, no orderbook call.
depth_fetched = true   → tier-2 row: liquidity_at_5c/10c computed from a live /orderbook fetch.

Existing rows are backfilled to true if liquidity_at_5c IS NOT NULL (they came from the old
flat-stream path which always fetched orderbooks), otherwise false.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "004_add_depth_fetched"
down_revision: Union[str, None] = "003_add_spread_column"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add column with default false
    op.add_column(
        "market_snapshots",
        sa.Column("depth_fetched", sa.Boolean(), nullable=False, server_default="false"),
    )

    # Backfill: existing rows that have liquidity values came from orderbook fetches
    op.execute(
        """
        UPDATE market_snapshots
        SET depth_fetched = true
        WHERE liquidity_at_5c IS NOT NULL
           OR liquidity_at_10c IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_column("market_snapshots", "depth_fetched")
