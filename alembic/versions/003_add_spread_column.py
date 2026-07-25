"""Add spread column to market_snapshots; backfill from existing bid/ask.

Revision ID: 003_add_spread_column
Revises: 113bf1b7a098
Create Date: 2026-07-25

The `spread` column stores raw (ask - bid) in probability units [0,1].
NULL when either side is missing. Persisted at write-time so the
`has_two_sided_book` spread cap can be re-tuned post-hoc without
re-fetching orderbooks.

Backfill: UPDATE market_snapshots SET spread = ask - bid WHERE both
bid and ask are non-NULL on existing rows. has_two_sided_book flags
on existing rows are left unchanged (they were set with the old
definition); re-ingestion or a separate UPDATE will correct them.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "003_add_spread_column"
down_revision: Union[str, None] = "113bf1b7a098"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add the spread column (nullable — existing rows will be NULL until backfill)
    op.add_column(
        "market_snapshots",
        sa.Column("spread", sa.Float(), nullable=True),
    )

    # 2. Backfill spread on all existing rows where both bid and ask are present
    op.execute(
        """
        UPDATE market_snapshots
        SET spread = ask - bid
        WHERE bid IS NOT NULL
          AND ask IS NOT NULL
        """
    )

    # 3. Correct has_two_sided_book on existing rows using default 0.10 spread cap
    op.execute(
        """
        UPDATE market_snapshots
        SET has_two_sided_book =
            CASE
                WHEN bid IS NOT NULL
                 AND ask IS NOT NULL
                 AND (ask - bid) <= 0.10
                THEN true
                ELSE false
            END
        """
    )


def downgrade() -> None:
    op.drop_column("market_snapshots", "spread")
