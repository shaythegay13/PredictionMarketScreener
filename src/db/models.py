"""SQLAlchemy 2.0 declarative database models."""

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    ARRAY,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for SQLAlchemy declarative models."""

    pass


class Market(Base):
    """Slowly-changing dimension table for unified markets across venues."""

    __tablename__ = "markets"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    venue: Mapped[str] = mapped_column(String(32), nullable=False)
    venue_market_id: Mapped[str] = mapped_column(String(256), nullable=False)

    event_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    series_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)

    title: Mapped[str] = mapped_column(Text, nullable=False)
    subtitle: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    outcomes: Mapped[List[str]] = mapped_column(ARRAY(Text), nullable=False)
    clob_token_ids: Mapped[List[str]] = mapped_column(ARRAY(Text), nullable=False)

    resolution_rules_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolution_source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    price_level_structure: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    # none_as_null=True: SQLAlchemy's JSON/JSONB types otherwise store a Python
    # None as the JSON scalar 'null' rather than a true SQL NULL — a 4-byte
    # jsonb value, not an actual empty column. This is purely a Python-side
    # bind-parameter behavior; it changes no DDL.
    price_ranges: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB(none_as_null=True), nullable=True)
    raw_market: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB(none_as_null=True), nullable=True)

    open_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    close_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    expected_resolution_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")

    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    snapshots: Mapped[List["MarketSnapshot"]] = relationship(
        "MarketSnapshot", back_populates="market", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("venue", "venue_market_id", name="uq_venue_market_id"),
        Index("ix_markets_venue_status", "venue", "status"),
    )


class MarketSnapshot(Base):
    """Append-only time series of per-outcome prices, volume, liquidity, and orderbooks."""

    __tablename__ = "market_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    market_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("markets.id", ondelete="CASCADE"), nullable=False
    )
    outcome_index: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome_label: Mapped[str] = mapped_column(String(256), nullable=False)

    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, nullable=False
    )

    bid: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ask: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    mid: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    last_trade_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    volume_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volume_total: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    open_interest: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    liquidity_at_5c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liquidity_at_10c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Raw ask - bid in [0,1]; NULL if either side missing. Stored for post-hoc spread-cap tuning.
    spread: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    raw_orderbook: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB(none_as_null=True), nullable=True)
    raw_market: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB(none_as_null=True), nullable=True)
    has_two_sided_book: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # True when liquidity_at_5c/10c were computed from a live /orderbook call (tier-2).
    # False for tier-1 rows where quotes come from the /events nested market object.
    depth_fetched: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Relationship
    market: Mapped["Market"] = relationship("Market", back_populates="snapshots")

    __table_args__ = (
        UniqueConstraint(
            "market_id",
            "outcome_index",
            "captured_at",
            name="uq_snapshot_market_outcome_captured",
        ),
        Index("ix_snapshots_market_captured", "market_id", captured_at.desc()),
        Index("ix_snapshots_captured", "captured_at"),
        {
            "postgresql_partition_by": "RANGE (captured_at)",
        },
    )


class CrossVenueCandidate(Base):
    """Stub table for potential cross-venue market matches."""

    __tablename__ = "cross_venue_candidates"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    kalshi_market_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("markets.id", ondelete="CASCADE"), nullable=False
    )
    polymarket_market_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("markets.id", ondelete="CASCADE"), nullable=False
    )

    similarity_score: Mapped[float] = mapped_column(Float, nullable=False)
    match_method: Mapped[str] = mapped_column(String(64), nullable=False, default="title_overlap")
    confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "kalshi_market_id", "polymarket_market_id", name="uq_cross_venue_pair"
        ),
        Index("ix_candidates_similarity", similarity_score.desc()),
    )
