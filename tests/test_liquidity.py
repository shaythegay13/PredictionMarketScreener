"""Unit tests for liquidity depth calculations."""

from src.analytics.liquidity import calculate_liquidity_depth, calculate_mid_price


def test_calculate_mid_price_two_sided():
    assert calculate_mid_price(0.40, 0.50) == 0.45


def test_calculate_mid_price_one_sided_or_empty():
    assert calculate_mid_price(0.40, None) is None
    assert calculate_mid_price(None, 0.50) is None
    assert calculate_mid_price(None, None) is None


def test_empty_orderbook_returns_none():
    mid = calculate_mid_price(None, None)
    assert mid is None
    assert calculate_liquidity_depth([], [], mid, delta=0.05) is None
    assert calculate_liquidity_depth([], [], mid, delta=0.10) is None


def test_one_sided_orderbook_returns_none():
    bids = [(0.40, 100.0)]
    asks = []
    mid = calculate_mid_price(0.40, None)
    assert mid is None
    assert calculate_liquidity_depth(bids, asks, mid, delta=0.05) is None
    assert calculate_liquidity_depth(bids, asks, mid, delta=0.10) is None


def test_calculate_liquidity_depth_two_sided():
    bids = [(0.48, 100.0), (0.45, 200.0), (0.35, 500.0)]
    asks = [(0.52, 100.0), (0.55, 200.0), (0.65, 500.0)]
    mid = calculate_mid_price(0.48, 0.52)
    assert mid == 0.50

    # Delta 0.05 => range [0.45, 0.55]
    depth_5c = calculate_liquidity_depth(bids, asks, mid, delta=0.05)
    assert depth_5c == 300.0

    depth_10c = calculate_liquidity_depth(bids, asks, mid, delta=0.10)
    assert depth_10c == 300.0


def test_calculate_liquidity_depth_decicent_boundary():
    # Test that tick boundaries (e.g. 0.4335 for mid=0.4835 and delta=0.05)
    # are correctly included due to epsilon tolerance.
    bids = [(0.4335, 1000.0)]
    asks = [(0.5335, 1000.0)]
    mid = 0.4835
    depth_5c = calculate_liquidity_depth(bids, asks, mid, delta=0.05)
    assert depth_5c == 967.0


def test_calculate_liquidity_depth_decicent_rounding_up():
    # mid_price = 0.5835, delta = 0.05 => min_price is float 0.5335000000000001.
    # Epsilon tolerance ensures the bid at 0.5335 is correctly included.
    bids = [(0.5335, 1000.0)]
    asks = []
    mid = 0.5835
    depth_5c = calculate_liquidity_depth(bids, asks, mid, delta=0.05)
    assert depth_5c == 533.5
