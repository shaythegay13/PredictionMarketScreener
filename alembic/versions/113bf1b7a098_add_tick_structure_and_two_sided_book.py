"""add tick structure and two sided book

Revision ID: 113bf1b7a098
Revises: 001_initial_schema
Create Date: 2026-07-24 14:47:50.848879

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '113bf1b7a098'
down_revision: Union[str, Sequence[str], None] = '001_initial_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


from sqlalchemy.dialects import postgresql

def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("markets", sa.Column("price_level_structure", sa.String(length=64), nullable=True))
    op.add_column("markets", sa.Column("price_ranges", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("market_snapshots", sa.Column("has_two_sided_book", sa.Boolean(), nullable=False, server_default=sa.text("false")))
    op.add_column("market_snapshots", sa.Column("raw_market", postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("market_snapshots", "raw_market")
    op.drop_column("market_snapshots", "has_two_sided_book")
    op.drop_column("markets", "price_ranges")
    op.drop_column("markets", "price_level_structure")
