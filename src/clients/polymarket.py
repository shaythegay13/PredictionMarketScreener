"""Async Polymarket venue API client integrating Gamma API (metadata) and CLOB API (orderbooks)."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from src.analytics.liquidity import calculate_liquidity_depth, calculate_mid_price
from src.clients.base import BaseVenueClient, NormalizedMarketDTO, NormalizedSnapshotDTO
from src.clients.rate_limiter import AsyncTokenBucket

logger = logging.getLogger(__name__)


def parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime string to UTC datetime."""
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
    """Safely parse float value."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


class PolymarketClient(BaseVenueClient):
    """Client for Polymarket Gamma API and CLOB Orderbook API."""

    GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
    CLOB_BASE_URL = "https://clob.polymarket.com"

    def __init__(
        self,
        rate_limit_rps: float = 10.0,
        http_client: Optional[httpx.AsyncClient] = None,
        timeout: float = 30.0,
        max_spread: float = 0.10,
    ):
        self.rate_limiter = AsyncTokenBucket(rate_limit_rps=rate_limit_rps)
        self.timeout = timeout
        self._external_client = http_client
        # has_two_sided_book is True only when bid AND ask exist AND spread <= max_spread.
        self.max_spread = max_spread

    async def _fetch_json(
        self, client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Execute rate-limited GET request."""
        await self.rate_limiter.acquire()
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    async def fetch_open_markets(self, limit: int = 50) -> List[NormalizedMarketDTO]:
        """Fetch active open markets from Polymarket Gamma API."""
        markets: List[NormalizedMarketDTO] = []
        close_client = False
        client = self._external_client
        if not client:
            client = httpx.AsyncClient(timeout=self.timeout, headers={"User-Agent": "PM-Screener/1.0"})
            close_client = True

        try:
            params = {
                "active": "true",
                "closed": "false",
                "limit": limit,
                "order": "volume",
                "ascending": "false",
            }
            url = f"{self.GAMMA_BASE_URL}/markets"
            batch = await self._fetch_json(client, url, params=params)

            if isinstance(batch, list):
                for m in batch:
                    market_id = str(m.get("id"))
                    if not market_id:
                        continue

                    outcomes_raw = m.get("outcomes", "[]")
                    tokens_raw = m.get("clobTokenIds", "[]")

                    outcomes: List[str] = (
                        json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
                    )
                    tokens: List[str] = (
                        json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
                    )

                    if not outcomes:
                        outcomes = ["Yes", "No"]
                    if not tokens:
                        tokens = []

                    events = m.get("events", [])
                    event_id = str(events[0].get("id")) if events and isinstance(events[0], dict) else None

                    title = m.get("question") or m.get("title") or ""
                    subtitle = m.get("groupItemTitle")
                    resolution_rules = m.get("description")
                    resolution_src = m.get("resolutionSource")

                    status = "active" if m.get("active") and not m.get("closed") else "closed"

                    normalized_m = NormalizedMarketDTO(
                        venue="polymarket",
                        venue_market_id=market_id,
                        event_id=event_id,
                        series_id=None,
                        title=title,
                        subtitle=subtitle,
                        outcomes=outcomes,
                        clob_token_ids=tokens,
                        resolution_rules_text=resolution_rules,
                        resolution_source=resolution_src,
                        open_time=parse_iso_datetime(m.get("startDate")),
                        close_time=parse_iso_datetime(m.get("endDate")),
                        expected_resolution_time=parse_iso_datetime(m.get("endDate")),
                        status=status,
                        raw_market=m,
                    )
                    setattr(normalized_m, "_gamma_data", m)
                    markets.append(normalized_m)

            logger.info("Fetched %d open Polymarket markets", len(markets))
            return markets
        finally:
            if close_client:
                await client.aclose()

    async def fetch_snapshots_for_market(
        self,
        client: httpx.AsyncClient,
        market: NormalizedMarketDTO,
        cycle_timestamp: datetime,
    ) -> List[NormalizedSnapshotDTO]:
        """Fetch orderbook for each outcome in parallel outcome/clob_token_ids arrays."""
        gamma_data: Dict[str, Any] = getattr(market, "_gamma_data", {})
        volume_24h = parse_float(gamma_data.get("volume24hr") or gamma_data.get("volume24hrClob"))
        volume_total = parse_float(gamma_data.get("volume") or gamma_data.get("volumeClob"))
        open_interest = parse_float(gamma_data.get("openInterest"))
        gamma_last_price = parse_float(gamma_data.get("lastTradePrice"))

        snapshots: List[NormalizedSnapshotDTO] = []

        outcomes = market.outcomes
        tokens = market.clob_token_ids

        for idx, outcome_label in enumerate(outcomes):
            token_id = tokens[idx] if idx < len(tokens) else None

            if not token_id:
                snapshots.append(
                    NormalizedSnapshotDTO(
                        venue_market_id=market.venue_market_id,
                        outcome_index=idx,
                        outcome_label=outcome_label,
                        captured_at=cycle_timestamp,
                        last_trade_price=gamma_last_price,
                        volume_24h=volume_24h,
                        volume_total=volume_total,
                        open_interest=open_interest,
                        raw_orderbook={"warning": "Missing token ID for outcome"},
                        has_two_sided_book=False,
                        raw_market=gamma_data,
                    )
                )
                continue

            try:
                url = f"{self.CLOB_BASE_URL}/book"
                clob_ob = await self._fetch_json(client, url, params={"token_id": token_id})

                raw_bids = clob_ob.get("bids", [])
                raw_asks = clob_ob.get("asks", [])
                clob_last_price = parse_float(clob_ob.get("last_trade_price")) or gamma_last_price

                bids: List[Tuple[float, float]] = []
                asks: List[Tuple[float, float]] = []

                for b in raw_bids:
                    p = parse_float(b.get("price"))
                    s = parse_float(b.get("size"))
                    if p is not None and s is not None:
                        bids.append((p, s))

                for a in raw_asks:
                    p = parse_float(a.get("price"))
                    s = parse_float(a.get("size"))
                    if p is not None and s is not None:
                        asks.append((p, s))

                bids.sort(key=lambda x: x[0], reverse=True)
                asks.sort(key=lambda x: x[0])

                best_bid = bids[0][0] if bids else None
                best_ask = asks[0][0] if asks else None
                mid = calculate_mid_price(best_bid, best_ask)

                # Compute raw spread and apply configurable spread cap.
                spread = round(best_ask - best_bid, 4) if (best_bid is not None and best_ask is not None) else None
                two_sided = (
                    best_bid is not None
                    and best_ask is not None
                    and spread is not None
                    and spread <= self.max_spread
                )

                liq_5c = calculate_liquidity_depth(bids, asks, mid, delta=0.05)
                liq_10c = calculate_liquidity_depth(bids, asks, mid, delta=0.10)

                raw_ob = {
                    "bids": [{"price": b[0], "size": b[1]} for b in bids[:10]],
                    "asks": [{"price": a[0], "size": a[1]} for a in asks[:10]],
                    "token_id": token_id,
                    "outcome_label": outcome_label,
                }

                snapshots.append(
                    NormalizedSnapshotDTO(
                        venue_market_id=market.venue_market_id,
                        outcome_index=idx,
                        outcome_label=outcome_label,
                        captured_at=cycle_timestamp,
                        bid=best_bid,
                        ask=best_ask,
                        mid=mid,
                        last_trade_price=clob_last_price,
                        volume_24h=volume_24h,
                        volume_total=volume_total,
                        open_interest=open_interest,
                        liquidity_at_5c=liq_5c,
                        liquidity_at_10c=liq_10c,
                        spread=spread,
                        raw_orderbook=raw_ob,
                        has_two_sided_book=two_sided,
                        depth_fetched=True,
                        raw_market=gamma_data,
                    )
                )
            except Exception as exc:
                logger.warning(
                    "Error fetching CLOB orderbook for Polymarket token %s: %s", token_id, exc
                )
                snapshots.append(
                    NormalizedSnapshotDTO(
                        venue_market_id=market.venue_market_id,
                        outcome_index=idx,
                        outcome_label=outcome_label,
                        captured_at=cycle_timestamp,
                        last_trade_price=gamma_last_price,
                        volume_24h=volume_24h,
                        volume_total=volume_total,
                        open_interest=open_interest,
                        raw_orderbook={"error": str(exc), "token_id": token_id},
                        has_two_sided_book=False,
                        raw_market=gamma_data,
                    )
                )

        return snapshots

    async def fetch_snapshots_for_markets(
        self, markets: List[NormalizedMarketDTO], cycle_timestamp: datetime
    ) -> List[NormalizedSnapshotDTO]:
        """Fetch snapshots concurrently for all Polymarket markets."""
        close_client = False
        client = self._external_client
        if not client:
            client = httpx.AsyncClient(timeout=self.timeout, headers={"User-Agent": "PM-Screener/1.0"})
            close_client = True

        try:
            tasks = [
                self.fetch_snapshots_for_market(client, market, cycle_timestamp)
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
