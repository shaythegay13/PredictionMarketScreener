"""Async Kalshi venue API client — event-anchored ingestion.

Ingestion strategy
------------------
Tier-1 (shallow, 15-min default):
    GET /events?status=open&with_nested_markets=true, paginate to cursor exhaustion.
    The nested market objects include yes_bid_dollars / yes_ask_dollars /
    no_bid_dollars / no_ask_dollars — full top-of-book quotes without a separate
    /orderbook request. liquidity_at_Nc is NULL; depth_fetched = false.

    The flat /markets stream is intentionally NOT used for ingestion. It is
    dominated by combinatorial multi-leg esports parlay contracts that have no
    /events entry and no tradeable liquidity. fetch_open_markets() is retained
    for ad-hoc single-ticker lookups only.

Tier-2 (deep, 5-min default):
    For markets passing the gate (bid AND ask both present AND spread <= max_spread),
    fetch /orderbook, compute liquidity_at_5c / liquidity_at_10c, depth_fetched = true.

Rate limiting
-------------
Default 2 RPS (500 ms between requests). 429 responses trigger exponential backoff
starting at 2 s, doubling on each retry (max 3 retries).

Kill switch
-----------
Every cursor loop guards against unbounded pagination via max_pages (default 1,000).
If the limit is hit, an ERROR is logged and the loop aborts cleanly.

Cent-denominated field watchdog
---------------------------------
Removed per Kalshi changelog Jan 15 2026 — https://docs.kalshi.com/changelog
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

from src.analytics.liquidity import calculate_liquidity_depth, calculate_mid_price
from src.clients.base import BaseVenueClient, NormalizedMarketDTO, NormalizedSnapshotDTO
from src.clients.rate_limiter import AsyncTokenBucket

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime string to timezone-aware UTC datetime."""
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def parse_float(val: Any) -> Optional[float]:
    """Safely parse a float value, returning None on missing/invalid input."""
    if val is None or val == "":
        return None
    try:
        return round(float(val), 4)
    except (ValueError, TypeError):
        return None


def check_for_legacy_keys(data: Any, context_info: str) -> None:
    """Recursively check for legacy integer cent fields and log a WARNING if found.

    Cent-denominated fields removed from Market responses per Kalshi changelog,
    release Jan 15 2026 — https://docs.kalshi.com/changelog
    """
    legacy_keys = {"yes_bid", "yes_ask", "last_price", "no_bid", "no_ask"}
    if isinstance(data, dict):
        found = legacy_keys.intersection(data.keys())
        if found:
            logger.warning(
                "WARNING: Legacy integer cent fields %s found in Kalshi API response for %s: %s. "
                "Cent-denominated fields removed from Market responses per Kalshi changelog, "
                "release Jan 15 2026 — https://docs.kalshi.com/changelog",
                list(found),
                context_info,
                {k: data[k] for k in found},
            )
        for val in data.values():
            check_for_legacy_keys(val, context_info)
    elif isinstance(data, list):
        for item in data:
            check_for_legacy_keys(item, context_info)


def _format_resolution_source(settlement_sources: Optional[List[Dict[str, Any]]]) -> Optional[str]:
    """Join an event's settlement_sources array into a single text field.

    Kalshi carries resolution source on the EVENT object (settlement_sources:
    [{"name": ..., "url": ...}, ...]), not on the market. An event can list
    anywhere from one to dozens of sources, so we join them rather than pick
    an arbitrary one. Returns None if the array is absent or empty.
    """
    if not settlement_sources:
        return None
    parts: List[str] = []
    seen = set()
    for src in settlement_sources:
        if not isinstance(src, dict):
            continue
        name = (src.get("name") or "").strip()
        url = (src.get("url") or "").strip()
        if not name and not url:
            continue
        label = f"{name}: {url}" if name and url else (name or url)
        if label not in seen:
            seen.add(label)
            parts.append(label)
    return "; ".join(parts) if parts else None


def _market_dto_from_raw(
    raw_market: Dict[str, Any],
    event_ticker: Optional[str] = None,
    settlement_sources: Optional[List[Dict[str, Any]]] = None,
) -> NormalizedMarketDTO:
    """Build a NormalizedMarketDTO from a raw Kalshi market dict (nested or flat)."""
    ticker = raw_market.get("ticker", "")
    rules_p = raw_market.get("rules_primary", "") or ""
    rules_s = raw_market.get("rules_secondary", "") or ""
    rules_text = (rules_p + "\n" + rules_s).strip() or None

    dto = NormalizedMarketDTO(
        venue="kalshi",
        venue_market_id=ticker,
        event_id=event_ticker or raw_market.get("event_ticker"),
        series_id=raw_market.get("mve_collection_ticker"),
        title=raw_market.get("title", ""),
        subtitle=raw_market.get("yes_sub_title") or raw_market.get("no_sub_title"),
        outcomes=["Yes", "No"],
        clob_token_ids=[f"{ticker}_YES", f"{ticker}_NO"],
        resolution_rules_text=rules_text,
        resolution_source=_format_resolution_source(settlement_sources),
        open_time=parse_datetime(raw_market.get("open_time")),
        close_time=parse_datetime(raw_market.get("close_time")),
        expected_resolution_time=parse_datetime(
            raw_market.get("expected_expiration_time") or raw_market.get("expiration_time")
        ),
        status=raw_market.get("status", "active"),
        price_level_structure=raw_market.get("price_level_structure"),
        price_ranges=raw_market.get("price_ranges"),
        raw_market=raw_market,
    )
    # Stash raw data for snapshot building
    setattr(dto, "_raw_kalshi_data", raw_market)
    return dto


def _tier1_snapshots_from_market(
    raw_market: Dict[str, Any],
    cycle_timestamp: datetime,
    max_spread: float,
) -> List[NormalizedSnapshotDTO]:
    """Build tier-1 (depth_fetched=False) snapshots from market-level quote fields.

    Uses yes_bid_dollars / yes_ask_dollars / no_bid_dollars / no_ask_dollars
    from the nested market object — no /orderbook request needed.
    liquidity_at_5c and liquidity_at_10c are NULL (not computed at tier-1).
    """
    ticker = raw_market.get("ticker", "")
    check_for_legacy_keys(raw_market, ticker)

    yes_bid = parse_float(raw_market.get("yes_bid_dollars"))
    yes_ask = parse_float(raw_market.get("yes_ask_dollars"))
    no_bid  = parse_float(raw_market.get("no_bid_dollars"))
    no_ask  = parse_float(raw_market.get("no_ask_dollars"))

    # Normalise zero-priced quotes to None — 0.0000 means no resting order
    if yes_bid == 0.0:
        yes_bid = None
    if yes_ask == 0.0:
        yes_ask = None
    if no_bid == 0.0:
        no_bid = None
    if no_ask == 0.0:
        no_ask = None

    yes_mid = calculate_mid_price(yes_bid, yes_ask)
    no_mid  = calculate_mid_price(no_bid, no_ask)

    yes_spread = round(yes_ask - yes_bid, 4) if (yes_bid is not None and yes_ask is not None) else None
    no_spread  = round(no_ask  - no_bid,  4) if (no_bid  is not None and no_ask  is not None) else None

    has_two_sided = (
        yes_bid is not None
        and yes_ask is not None
        and yes_spread is not None
        and yes_spread <= max_spread
    )

    volume_24h    = parse_float(raw_market.get("volume_24h_fp"))
    volume_total  = parse_float(raw_market.get("volume_fp"))
    open_interest = parse_float(raw_market.get("open_interest_fp"))
    last_trade    = parse_float(raw_market.get("last_price_dollars"))

    snap_yes = NormalizedSnapshotDTO(
        venue_market_id=ticker,
        outcome_index=0,
        outcome_label="Yes",
        captured_at=cycle_timestamp,
        bid=yes_bid,
        ask=yes_ask,
        mid=yes_mid,
        last_trade_price=last_trade,
        volume_24h=volume_24h,
        volume_total=volume_total,
        open_interest=open_interest,
        spread=yes_spread,
        has_two_sided_book=has_two_sided,
        depth_fetched=False,
        raw_market=raw_market,
    )

    snap_no = NormalizedSnapshotDTO(
        venue_market_id=ticker,
        outcome_index=1,
        outcome_label="No",
        captured_at=cycle_timestamp,
        bid=no_bid,
        ask=no_ask,
        mid=no_mid,
        last_trade_price=round(1.0 - last_trade, 4) if last_trade is not None else None,
        volume_24h=volume_24h,
        volume_total=volume_total,
        open_interest=open_interest,
        spread=no_spread,
        has_two_sided_book=has_two_sided,
        depth_fetched=False,
        raw_market=raw_market,
    )

    return [snap_yes, snap_no]


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class KalshiClient(BaseVenueClient):
    """Client for Kalshi elections / trade API v2.

    Primary ingestion path: fetch_events_with_snapshots() — event-anchored,
    returns (markets, tier-1-snapshots) in a single paginated pass.

    Tier-2 depth: fetch_orderbook_snapshots_for_markets() — for gate-passing
    markets only; adds liquidity_at_Nc and sets depth_fetched=True.
    """

    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

    def __init__(
        self,
        rate_limit_rps: float = 2.0,
        http_client: Optional[httpx.AsyncClient] = None,
        timeout: float = 30.0,
        max_spread: float = 0.10,
        max_pages: int = 1000,
        min_volume_for_tier2: float = 0.0,
        excluded_categories: Optional[Set[str]] = None,
    ):
        self.rate_limiter = AsyncTokenBucket(rate_limit_rps=rate_limit_rps)
        self.timeout = timeout
        self._external_client = http_client
        # has_two_sided_book = True only when bid AND ask exist AND spread <= max_spread
        self.max_spread = max_spread
        # Kill switch: abort cursor loop and log ERROR if page count exceeds this
        self.max_pages = max_pages
        # Minimum volume_fp for a market to qualify for tier-2 orderbook fetch
        self.min_volume_for_tier2 = min_volume_for_tier2
        # Event categories (Kalshi's own `category` field, e.g. "Sports") to
        # exclude entirely from tier-1 ingestion. Esports is filed under
        # "Sports" too — there is no separate esports category.
        self.excluded_categories = excluded_categories or set()

    # ------------------------------------------------------------------
    # Internal HTTP
    # ------------------------------------------------------------------

    async def _fetch_json(
        self, client: httpx.AsyncClient, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute rate-limited GET with exponential backoff on 429."""
        max_retries = 3
        backoff = 2.0

        for attempt in range(max_retries):
            await self.rate_limiter.acquire()
            url = f"{self.BASE_URL}{endpoint}"
            resp = await client.get(url, params=params)

            if resp.status_code == 429:
                logger.warning(
                    "Kalshi 429 Rate Limit hit on %s. Backing off %.1fs (attempt %d/%d)...",
                    endpoint, backoff, attempt + 1, max_retries,
                )
                await asyncio.sleep(backoff)
                backoff *= 2
                continue

            resp.raise_for_status()
            js = resp.json()
            check_for_legacy_keys(js, endpoint)
            return js

        # Final attempt after retries exhausted
        await self.rate_limiter.acquire()
        url = f"{self.BASE_URL}{endpoint}"
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        js = resp.json()
        check_for_legacy_keys(js, endpoint)
        return js

    def _make_client(self) -> Tuple[httpx.AsyncClient, bool]:
        """Return (client, should_close). Reuses external client when provided."""
        if self._external_client:
            return self._external_client, False
        return httpx.AsyncClient(
            timeout=self.timeout, headers={"User-Agent": "PM-Screener/1.0"}
        ), True

    # ------------------------------------------------------------------
    # Tier-1: event-anchored ingestion
    # ------------------------------------------------------------------

    async def fetch_events_with_snapshots(
        self, cycle_timestamp: datetime
    ) -> Tuple[List[NormalizedMarketDTO], List[NormalizedSnapshotDTO]]:
        """Paginate /events?with_nested_markets=true to exhaustion.

        Returns (all_markets, all_tier1_snapshots). Each nested market produces
        two snapshot rows (Yes/No) using market-level quote fields only.
        depth_fetched=False on all returned snapshots.

        Aborts with an ERROR log if page count exceeds self.max_pages.
        """
        client, close_client = self._make_client()
        all_markets: List[NormalizedMarketDTO] = []
        all_snapshots: List[NormalizedSnapshotDTO] = []

        try:
            cursor: Optional[str] = None
            page = 0

            while True:
                page += 1
                if page > self.max_pages:
                    logger.error(
                        "KalshiClient.fetch_events_with_snapshots: max_pages kill switch "
                        "triggered at page %d (limit=%d). Aborting cursor loop. "
                        "This should never happen for /events — investigate API behaviour.",
                        page, self.max_pages,
                    )
                    break

                params: Dict[str, Any] = {
                    "limit": 200,
                    "status": "open",
                    "with_nested_markets": "true",
                }
                if cursor:
                    params["cursor"] = cursor

                data = await self._fetch_json(client, "/events", params=params)
                events_batch = data.get("events", [])

                if not events_batch:
                    break

                for event in events_batch:
                    if event.get("category") in self.excluded_categories:
                        continue
                    nested_markets: List[Dict[str, Any]] = event.get("markets", [])
                    settlement_sources = event.get("settlement_sources")
                    for raw_market in nested_markets:
                        try:
                            dto = _market_dto_from_raw(
                                raw_market,
                                event_ticker=event.get("event_ticker"),
                                settlement_sources=settlement_sources,
                            )
                            snaps = _tier1_snapshots_from_market(
                                raw_market, cycle_timestamp, self.max_spread
                            )
                            all_markets.append(dto)
                            all_snapshots.extend(snaps)
                        except Exception as exc:
                            ticker = raw_market.get("ticker", "?")
                            logger.warning(
                                "Skipping nested market %s in event %s: %s",
                                ticker, event.get("event_ticker", "?"), exc,
                            )

                cursor = data.get("cursor") or ""
                if not cursor:
                    logger.info(
                        "KalshiClient tier-1: cursor exhausted after %d pages. "
                        "%d markets, %d snapshots.",
                        page, len(all_markets), len(all_snapshots),
                    )
                    break

            return all_markets, all_snapshots

        finally:
            if close_client:
                await client.aclose()

    # ------------------------------------------------------------------
    # Tier-2: orderbook depth for gate-passing markets
    # ------------------------------------------------------------------

    async def fetch_orderbook_snapshots_for_markets(
        self,
        markets: List[NormalizedMarketDTO],
        cycle_timestamp: datetime,
    ) -> List[NormalizedSnapshotDTO]:
        """Fetch /orderbook for each market and return depth-enriched snapshots.

        depth_fetched=True, liquidity_at_5c/10c populated.
        Called only for markets that passed the tier-2 gate in the runner.
        """
        client, close_client = self._make_client()
        try:
            tasks = [
                self._fetch_orderbook_for_market(client, market, cycle_timestamp)
                for market in markets
            ]
            results = await asyncio.gather(*tasks)
            all_snapshots: List[NormalizedSnapshotDTO] = []
            for res in results:
                all_snapshots.extend(res)
            return all_snapshots
        finally:
            if close_client:
                await client.aclose()

    async def _fetch_orderbook_for_market(
        self,
        client: httpx.AsyncClient,
        market: NormalizedMarketDTO,
        cycle_timestamp: datetime,
    ) -> List[NormalizedSnapshotDTO]:
        """Fetch /orderbook for a single market; return two depth-enriched snapshot DTOs."""
        ticker = market.venue_market_id
        raw_market: Dict[str, Any] = getattr(market, "_raw_kalshi_data", {})

        volume_24h    = parse_float(raw_market.get("volume_24h_fp"))
        volume_total  = parse_float(raw_market.get("volume_fp"))
        open_interest = parse_float(raw_market.get("open_interest_fp"))
        last_trade    = parse_float(raw_market.get("last_price_dollars"))

        try:
            ob_data = await self._fetch_json(client, f"/markets/{ticker}/orderbook")
            fp = ob_data.get("orderbook_fp", ob_data.get("orderbook", {}))

            yes_dollar_levels: List[List[Any]] = fp.get("yes_dollars", fp.get("yes", []))
            no_dollar_levels:  List[List[Any]] = fp.get("no_dollars",  fp.get("no",  []))

            # yes_dollars: ascending bid stack (worst→best); no_dollars: same.
            # Best yes bid  = yes_dollars[-1][0]
            # Best yes ask  = 1 - no_dollars[-1][0]   (equiv. best no bid)
            yes_bids: List[Tuple[float, float]] = []
            yes_asks: List[Tuple[float, float]] = []

            for level in yes_dollar_levels:
                if len(level) >= 2:
                    p = parse_float(level[0])
                    s = parse_float(level[1])
                    if p is not None and s is not None:
                        yes_bids.append((p, s))

            for level in no_dollar_levels:
                if len(level) >= 2:
                    no_p = parse_float(level[0])
                    s    = parse_float(level[1])
                    if no_p is not None and s is not None:
                        yes_asks.append((round(1.0 - no_p, 4), s))

            yes_bids.sort(key=lambda x: x[0], reverse=True)   # best bid first
            yes_asks.sort(key=lambda x: x[0])                  # best ask first

            yes_bid = yes_bids[0][0] if yes_bids else None
            yes_ask = yes_asks[0][0] if yes_asks else None
            yes_mid = calculate_mid_price(yes_bid, yes_ask)

            yes_liq_5c  = calculate_liquidity_depth(yes_bids, yes_asks, yes_mid, delta=0.05)
            yes_liq_10c = calculate_liquidity_depth(yes_bids, yes_asks, yes_mid, delta=0.10)

            no_bid = round(1.0 - yes_ask, 4) if yes_ask is not None else None
            no_ask = round(1.0 - yes_bid, 4) if yes_bid is not None else None
            no_mid = calculate_mid_price(no_bid, no_ask)

            no_bids: List[Tuple[float, float]] = [
                (round(1.0 - a[0], 4), a[1]) for a in yes_asks
            ]
            no_asks: List[Tuple[float, float]] = [
                (round(1.0 - b[0], 4), b[1]) for b in yes_bids
            ]
            no_bids.sort(key=lambda x: x[0], reverse=True)
            no_asks.sort(key=lambda x: x[0])

            no_liq_5c  = calculate_liquidity_depth(no_bids, no_asks, no_mid, delta=0.05)
            no_liq_10c = calculate_liquidity_depth(no_bids, no_asks, no_mid, delta=0.10)

            yes_spread = round(yes_ask - yes_bid, 4) if (yes_bid is not None and yes_ask is not None) else None
            no_spread  = round(no_ask  - no_bid,  4) if (no_bid  is not None and no_ask  is not None) else None
            has_two_sided = (
                yes_bid is not None and yes_ask is not None
                and yes_spread is not None and yes_spread <= self.max_spread
            )

            raw_ob_yes = {
                "bids": [{"price": b[0], "size": b[1]} for b in yes_bids[:10]],
                "asks": [{"price": a[0], "size": a[1]} for a in yes_asks[:10]],
            }
            raw_ob_no = {
                "bids": [{"price": b[0], "size": b[1]} for b in no_bids[:10]],
                "asks": [{"price": a[0], "size": a[1]} for a in no_asks[:10]],
            }

            return [
                NormalizedSnapshotDTO(
                    venue_market_id=ticker,
                    outcome_index=0,
                    outcome_label="Yes",
                    captured_at=cycle_timestamp,
                    bid=yes_bid, ask=yes_ask, mid=yes_mid,
                    last_trade_price=last_trade,
                    volume_24h=volume_24h, volume_total=volume_total,
                    open_interest=open_interest,
                    liquidity_at_5c=yes_liq_5c,
                    liquidity_at_10c=yes_liq_10c,
                    spread=yes_spread,
                    raw_orderbook=raw_ob_yes,
                    has_two_sided_book=has_two_sided,
                    depth_fetched=True,
                    raw_market=raw_market,
                ),
                NormalizedSnapshotDTO(
                    venue_market_id=ticker,
                    outcome_index=1,
                    outcome_label="No",
                    captured_at=cycle_timestamp,
                    bid=no_bid, ask=no_ask, mid=no_mid,
                    last_trade_price=round(1.0 - last_trade, 4) if last_trade is not None else None,
                    volume_24h=volume_24h, volume_total=volume_total,
                    open_interest=open_interest,
                    liquidity_at_5c=no_liq_5c,
                    liquidity_at_10c=no_liq_10c,
                    spread=no_spread,
                    raw_orderbook=raw_ob_no,
                    has_two_sided_book=has_two_sided,
                    depth_fetched=True,
                    raw_market=raw_market,
                ),
            ]

        except Exception as exc:
            logger.warning("Error fetching orderbook for Kalshi ticker %s: %s", ticker, exc)
            return [
                NormalizedSnapshotDTO(
                    venue_market_id=ticker, outcome_index=0, outcome_label="Yes",
                    captured_at=cycle_timestamp,
                    raw_orderbook={"error": str(exc)},
                    has_two_sided_book=False, depth_fetched=False, raw_market=raw_market,
                ),
                NormalizedSnapshotDTO(
                    venue_market_id=ticker, outcome_index=1, outcome_label="No",
                    captured_at=cycle_timestamp,
                    raw_orderbook={"error": str(exc)},
                    has_two_sided_book=False, depth_fetched=False, raw_market=raw_market,
                ),
            ]

    # ------------------------------------------------------------------
    # BaseVenueClient interface — retained for ad-hoc/testing use only.
    # NOT called by the runner's ingestion loop.
    # ------------------------------------------------------------------

    async def fetch_open_markets(self, limit: int = 50) -> List[NormalizedMarketDTO]:
        """Ad-hoc market lookup via the flat /markets stream.

        NOT used in the ingestion loop. The flat stream is dominated by
        combinatorial esports parlay legs; use fetch_events_with_snapshots()
        for event-anchored ingestion instead.
        """
        client, close_client = self._make_client()
        try:
            data = await self._fetch_json(client, "/markets", params={"limit": limit, "status": "open"})
            markets = []
            for raw in data.get("markets", [])[:limit]:
                if raw.get("ticker"):
                    markets.append(_market_dto_from_raw(raw))
            return markets
        finally:
            if close_client:
                await client.aclose()

    async def fetch_snapshots_for_markets(
        self, markets: List[NormalizedMarketDTO], cycle_timestamp: datetime
    ) -> List[NormalizedSnapshotDTO]:
        """Ad-hoc orderbook fetch for a list of markets.

        Used by tests and one-off probes. The runner uses
        fetch_orderbook_snapshots_for_markets() for tier-2 instead.
        """
        return await self.fetch_orderbook_snapshots_for_markets(markets, cycle_timestamp)
