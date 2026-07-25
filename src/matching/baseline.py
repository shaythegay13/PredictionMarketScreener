"""Naive title-similarity baseline cross-venue candidate matcher."""

import logging
from typing import List, Tuple

from rapidfuzz import fuzz
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import CrossVenueCandidate, Market

logger = logging.getLogger(__name__)


def compute_title_similarity(title1: str, title2: str) -> float:
    """Compute normalized title similarity score in range [0.0, 1.0]."""
    if not title1 or not title2:
        return 0.0
    # rapidfuzz ratio returns 0..100
    score = fuzz.token_sort_ratio(title1.lower(), title2.lower())
    return round(score / 100.0, 4)


async def generate_cross_venue_candidates(
    session: AsyncSession, min_similarity_threshold: float = 0.60
) -> int:
    """Query active Kalshi and Polymarket markets, compute title similarity, and save candidates.

    Returns count of new candidate pairs generated.
    """
    # Fetch all active Kalshi markets
    stmt_kalshi = select(Market).where(Market.venue == "kalshi", Market.status == "active")
    kalshi_markets = (await session.execute(stmt_kalshi)).scalars().all()

    # Fetch all active Polymarket markets
    stmt_poly = select(Market).where(Market.venue == "polymarket", Market.status == "active")
    poly_markets = (await session.execute(stmt_poly)).scalars().all()

    if not kalshi_markets or not poly_markets:
        logger.info("Insufficient active markets across both venues for matching.")
        return 0

    candidate_pairs: List[Tuple[Market, Market, float]] = []

    for km in kalshi_markets:
        for pm in poly_markets:
            sim = compute_title_similarity(km.title, pm.title)
            if sim >= min_similarity_threshold:
                candidate_pairs.append((km, pm, sim))

    if not candidate_pairs:
        logger.info("No candidates exceeded similarity threshold %.2f", min_similarity_threshold)
        return 0

    inserted_count = 0
    for km, pm, score in candidate_pairs:
        stmt = (
            insert(CrossVenueCandidate)
            .values(
                kalshi_market_id=km.id,
                polymarket_market_id=pm.id,
                similarity_score=score,
                match_method="naive_title_similarity",
                confirmed=False,
            )
            .on_conflict_do_update(
                constraint="uq_cross_venue_pair",
                set_={
                    "similarity_score": score,
                    "match_method": "naive_title_similarity",
                },
            )
        )
        await session.execute(stmt)
        inserted_count += 1

    await session.commit()
    logger.info("Generated / updated %d cross-venue candidate pairs", inserted_count)
    return inserted_count
