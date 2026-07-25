"""Liquidity depth and orderbook metric calculations."""

from typing import List, Optional, Tuple


def calculate_mid_price(
    bid: Optional[float], ask: Optional[float]
) -> Optional[float]:
    """Calculate mid price from best bid and ask.

    Returns None if either bid or ask is missing (one-sided or empty orderbook).
    Never falls back to arbitrary defaults or single-sided prices.
    """
    if bid is None or ask is None:
        return None
    return round((bid + ask) / 2.0, 4)


def calculate_liquidity_depth(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
    mid_price: Optional[float],
    delta: float,
) -> Optional[float]:
    """Calculate total notional dollar liquidity within mid +/- delta.

    bids/asks are lists of (price [0..1], size [notional or contracts]).
    Returns None if mid_price is None or if orderbook has no liquidity.
    """
    if mid_price is None or (not bids and not asks):
        return None

    min_price = max(0.0, mid_price - delta)
    max_price = min(1.0, mid_price + delta)

    notional = 0.0
    matched_levels = 0
    epsilon = 1e-9

    # Bid liquidity (buyers sitting at min_price <= price <= mid_price)
    for price, size in bids:
        if (min_price - epsilon) <= price <= (mid_price + epsilon):
            notional += price * size
            matched_levels += 1

    # Ask liquidity (sellers sitting at mid_price <= price <= max_price)
    for price, size in asks:
        if (mid_price - epsilon) <= price <= (max_price + epsilon):
            notional += price * size
            matched_levels += 1

    if matched_levels == 0:
        return None

    return round(notional, 4)
