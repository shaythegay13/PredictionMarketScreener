"""Initial schema creation for markets, market_snapshots (12-month partitioned), and cross_venue_candidates.

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-07-24 12:00:00.000000

"""

from typing import Sequence, Union
from datetime import datetime, timezone
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create markets table
    op.create_table(
        "markets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("venue", sa.String(length=32), nullable=False),
        sa.Column("venue_market_id", sa.String(length=256), nullable=False),
        sa.Column("event_id", sa.String(length=256), nullable=True),
        sa.Column("series_id", sa.String(length=256), nullable=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("subtitle", sa.Text(), nullable=True),
        sa.Column("outcomes", sa.ARRAY(sa.Text()), nullable=False),
        sa.Column("clob_token_ids", sa.ARRAY(sa.Text()), nullable=False),
        sa.Column("resolution_rules_text", sa.Text(), nullable=True),
        sa.Column("resolution_source", sa.Text(), nullable=True),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("close_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expected_resolution_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "last_updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("venue", "venue_market_id", name="uq_venue_market_id"),
    )
    op.create_index("ix_markets_venue_status", "markets", ["venue", "status"])

    # 2. Create partitioned market_snapshots table using raw DDL for PostgreSQL PARTITION BY RANGE
    op.execute(
        """
        CREATE TABLE market_snapshots (
            id UUID NOT NULL,
            market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
            outcome_index INTEGER NOT NULL,
            outcome_label VARCHAR(256) NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL,
            bid DOUBLE PRECISION,
            ask DOUBLE PRECISION,
            mid DOUBLE PRECISION,
            last_trade_price DOUBLE PRECISION,
            volume_24h DOUBLE PRECISION,
            volume_total DOUBLE PRECISION,
            open_interest DOUBLE PRECISION,
            liquidity_at_5c DOUBLE PRECISION,
            liquidity_at_10c DOUBLE PRECISION,
            raw_orderbook JSONB,
            PRIMARY KEY (id, captured_at),
            CONSTRAINT uq_snapshot_market_outcome_captured UNIQUE (market_id, outcome_index, captured_at)
        ) PARTITION BY RANGE (captured_at);
        """
    )
    op.execute(
        "CREATE INDEX ix_snapshots_market_captured ON market_snapshots (market_id, captured_at DESC);"
    )
    op.execute(
        "CREATE INDEX ix_snapshots_captured ON market_snapshots (captured_at);"
    )

    # Pre-create default partition and 12 monthly range partitions (2026-07 to 2027-07)
    op.execute(
        "CREATE TABLE market_snapshots_default PARTITION OF market_snapshots DEFAULT;"
    )

    partitions = [
        ("y2026m07", "2026-07-01", "2026-08-01"),
        ("y2026m08", "2026-08-01", "2026-09-01"),
        ("y2026m09", "2026-09-01", "2026-10-01"),
        ("y2026m10", "2026-10-01", "2026-11-01"),
        ("y2026m11", "2026-11-01", "2026-12-01"),
        ("y2026m12", "2026-12-01", "2027-01-01"),
        ("y2027m01", "2027-01-01", "2027-02-01"),
        ("y2027m02", "2027-02-01", "2027-03-01"),
        ("y2027m03", "2027-03-01", "2027-04-01"),
        ("y2027m04", "2027-04-01", "2027-05-01"),
        ("y2027m05", "2027-05-01", "2027-06-01"),
        ("y2027m06", "2027-06-01", "2027-07-01"),
        ("y2027m07", "2027-07-01", "2027-08-01"),
    ]

    for part_suffix, start_date, end_date in partitions:
        op.execute(
            f"""
            CREATE TABLE market_snapshots_{part_suffix} PARTITION OF market_snapshots
            FOR VALUES FROM ('{start_date} 00:00:00+00') TO ('{end_date} 00:00:00+00');
            """
        )

    # 3. Create cross_venue_candidates table
    op.create_table(
        "cross_venue_candidates",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("kalshi_market_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("polymarket_market_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("similarity_score", sa.Float(), nullable=False),
        sa.Column("match_method", sa.String(length=64), nullable=False, server_default="title_overlap"),
        sa.Column("confirmed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["kalshi_market_id"], ["markets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["polymarket_market_id"], ["markets.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("kalshi_market_id", "polymarket_market_id", name="uq_cross_venue_pair"),
    )
    op.create_index(
        "ix_candidates_similarity",
        "cross_venue_candidates",
        [sa.text("similarity_score DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_candidates_similarity", table_name="cross_venue_candidates")
    op.drop_table("cross_venue_candidates")
    op.execute("DROP TABLE IF EXISTS market_snapshots CASCADE;")
    op.drop_index("ix_markets_venue_status", table_name="markets")
    op.drop_table("markets")
