# Prediction Market Screener

An async multi-venue data ingestion and archival system for prediction market orderbooks. Supports Kalshi (Trade API v2) and Polymarket (Gamma + CLOB APIs).

## Architecture

```
src/
  config.py          — Pydantic-settings configuration (env / .env)
  runner.py          — Snapshot loop: partition check → fetch → upsert
  clients/
    base.py          — NormalizedMarketDTO, NormalizedSnapshotDTO, BaseVenueClient
    kalshi.py        — Kalshi venue client (tiered event-anchored, 2 RPS rate-limited)
    polymarket.py    — Polymarket Gamma + CLOB client
    rate_limiter.py  — AsyncTokenBucket token-bucket rate limiter
  analytics/
    liquidity.py     — calculate_mid_price, calculate_liquidity_depth
  matching/
    baseline.py      — Cross-venue title similarity
  db/
    models.py        — SQLAlchemy 2.0 ORM: Market, MarketSnapshot, CrossVenueCandidate
    session.py       — Async session factory
alembic/             — PostgreSQL migrations
tests/               — pytest-asyncio + respx unit test suite
```

## Quick Start

```bash
# Start Postgres
docker-compose up -d

# Apply migrations
alembic upgrade head

# Single ingestion cycle (both venues)
python -m src.runner --single-run

# Continuous loop with concurrent pipeline loops
python -m src.runner
```

## Configuration

All settings are read from environment variables or a `.env` file.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://...` | SQLAlchemy async DSN |
| `KALSHI_TIER1_INTERVAL_SECONDS` | `900` | Ingestion loop interval for Kalshi Tier-1 (15 min) |
| `KALSHI_TIER2_INTERVAL_SECONDS` | `300` | Ingestion loop interval for Kalshi Tier-2 (5 min) |
| `POLYMARKET_INTERVAL_SECONDS` | `300` | Ingestion loop interval for Polymarket (5 min) |
| `KALSHI_RATE_LIMIT_RPS` | `2.0` | Kalshi requests/second cap (500ms safety delay) |
| `POLYMARKET_RATE_LIMIT_RPS` | `5.0` | Polymarket requests/second cap |
| `MAX_SPREAD_FOR_TWO_SIDED` | `0.10` | Max (ask − bid) in probability units for `has_two_sided_book=True` |
| `MIN_VOLUME_FOR_TIER2` | `0.0` | Minimum volume_fp to qualify for Tier-2 orderbook fetch |
| `KALSHI_MAX_PAGES` | `1000` | Hard page cap kill switch on cursor pagination loops |
| `LOG_LEVEL` | `INFO` | Python logging level |

## Ingestion Strategy

### Kalshi (Tiered Ingestion)

Ingestion is event-anchored via `/events?with_nested_markets=true`.
The flat `/markets` stream is intentionally **not** used because it is dominated by millions of combinatorial parlay legs (`KXMVESPORTSMULTIGAMEEXTENDED-*`) that have no `/events` entry and no tradeable liquidity. Instead, we use a tiered strategy:

*   **Tier 1 (Shallow, 15 min):** Paginate `/events?with_nested_markets=true` to exhaustion to discover all genuine prediction markets and their top-of-book quotes (`yes_bid_dollars`, `yes_ask_dollars`, etc.). Snapshot rows are stored with `depth_fetched = false` and `liquidity_at_Nc = NULL`.
*   **Tier 2 (Deep, 5 min):** Find active markets whose latest snapshot is two-sided and narrow (`spread <= 0.10` and `volume >= MIN_VOLUME_FOR_TIER2`). For these gated markets, we fetch the full `/orderbook` depth and compute `liquidity_at_5c` / `liquidity_at_10c` with `depth_fetched = true`.

### Polymarket

Ingested on a 5-minute loop. Fetches open markets from the Gamma API, and fetches CLOB orderbook snapshots for all returned markets, setting `depth_fetched = true` upon success.

## `has_two_sided_book` Definition

A snapshot row is flagged `has_two_sided_book = true` if and only if:

1.  Both `bid` and `ask` are non-NULL, **and**
2.  `(ask − bid) ≤ MAX_SPREAD_FOR_TWO_SIDED` (default 0.10)

The raw `spread` value is stored unconditionally so the cap can be re-tuned at query time without re-fetching data:

```sql
-- Re-apply a tighter cap of 5¢ without re-ingesting
SELECT * FROM market_snapshots WHERE spread <= 0.05;
```

## Known Limitations

### `liquidity_at_5c` / `liquidity_at_10c` overstates depth for tail-priced markets

`liquidity_at_Nc` sums notional dollar value of all resting orders within ±N cents of mid price. For markets priced near 0 or 1 (e.g. a "Yes" leg at 0.997), a single large resting bid at 0.997 is included in both the ±5¢ and ±10¢ windows and inflates the metric to multi-million-dollar figures.

This liquidity is not practically accessible: a fill at 0.997 requires a counterparty willing to sell at that price, and the spread to the next real level can be the full remaining probability mass. The metric is arithmetically correct but economically misleading for tail-priced outcomes.

**Affected patterns:**

*   Kalshi esports near-resolved legs (e.g. bid=0.998, ask=None — not flagged `has_two_sided_book`)
*   Polymarket extreme legs (e.g. "Will Kendrick Lamar be top artist?" No side at 0.997/0.998)

**Workaround at query time:**

```sql
-- Exclude tail-priced rows before aggregating liquidity
SELECT * FROM market_snapshots
WHERE has_two_sided_book = true
  AND mid BETWEEN 0.10 AND 0.90;
```

### Cent-denominated fields

Kalshi removed cent-denominated integer fields (`yes_bid`, `yes_ask`, `last_price`, `no_bid`, `no_ask`) from Market responses per changelog release Jan 15 2026 (https://docs.kalshi.com/changelog). The client logs a `WARNING` if any such field reappears in a response.

### Known risk: host suspend on a local machine

Running the continuous loop on a laptop or a Linux-on-Chromebook (Crostini) container means the entire process can be paused for an arbitrary stretch whenever the host device sleeps — confirmed via `journalctl` (`maitred: Received request to prepare to suspend`) after observing all three loops (Kalshi Tier-1, Tier-2, Polymarket) go silent in lockstep and resume together after ~11 minutes with no errors. This isn't a code bug: a suspended VM runs nothing, ours or the kernel's, until it's resumed, so independent asyncio tasks naturally resume together. It just means ingestion cadence has silent gaps whenever the host sleeps. For genuinely continuous operation, run this on infrastructure that doesn't suspend (see `DEPLOY.md`).

## Running Tests

```bash
python -m pytest tests/ -v
```
