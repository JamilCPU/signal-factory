# Signal Factory — Replay Simulator (Phase 1) Design Spec

**Date:** 2026-06-02
**Status:** Approved design, pre-implementation
**Builds on:** Phase 0 Data Spine (`2026-06-01-signal-factory-design.md`)

---

## 1. Goal

A historical-replay paper trader where the **Claude model decides both entry and exit**, sees
**only data available at decision time** (strict no-lookahead), trades a **virtual portfolio**,
and produces an **accuracy + P&L report**. The point: measure whether the model's judgment
actually makes money, over many past trades, fast and for near-zero cost — before any live data
or real money is involved.

## 2. Decisions locked during brainstorming (2026-06-02)

| Decision | Choice |
|---|---|
| Test mode | **Historical replay first** (simulated clock walking forward), not live forward trading |
| Decider | **The Claude model** makes the calls, behind a swappable `Decider` interface |
| Signal | **SEC filings (8-K / Form 4)** + price history — free, full-history, matches the "business moves not social media" thesis |
| Exit | **Model decides the exit too** (full position lifecycle), re-consulted on a cadence; not mechanical |
| Cost control | Cadence (not removing the decision) + small ticker set + bounded window + max-hold backstop |
| Prices | Free daily bars (yfinance / Stooq) for v1; intraday is a later enhancement |
| Scope of phase | Replay only. No live feed, no Watcher daemon, no broker. |

## 3. The no-lookahead wall (the core invariant)

The model never touches the database directly. It only ever receives a
**`MarketView(as_of, ticker)`** — a read-only window that *can only return data timestamped
≤ as_of*: the triggering filing, earlier filings for context, and price history up to that
instant. The simulator constructs the MarketView from the simulated clock.

The future prices used to score a trade live on a **completely separate code path** the Decider
cannot reach. This separation is enforced in code (the Decider's only input is a `MarketView`)
and verified by tests (§7). This is the single most important property of the system: a backtest
that leaks the future is worse than useless because it lies to you.

## 4. Components (extending Phase 0)

- **Historical ingestors**
  - `EdgarHistorySource` — pulls past 8-K / Form 4 filings for the configured tickers from SEC
    EDGAR into the existing `events` table. Free, full history, precise timestamps.
  - `PriceHistoryLoader` — pulls daily OHLCV bars (yfinance/Stooq) into a new `prices` table.
  - Both honor the spec's swappable interfaces so paid/real-time sources can replace them later.

- **`MarketView`** — the as-of boundary of §3. Given `as_of` + `ticker`, exposes: the triggering
  event, prior events (≤ as_of), and price history (≤ as_of). The only object the Decider sees.

- **`Decider` interface + `ClaudeDecider`** — input: a `MarketView` + current `PositionState`;
  output: a structured `Decision(action = BUY | SELL | HOLD, conviction: float, reason: str)`.
  `ClaudeDecider` calls the Claude API; the interface keeps it swappable (a deterministic
  baseline decider can drop in for free harness validation).

- **Replay engine (the clock)** — walks events chronologically. At each decision point it builds
  `MarketView(as_of = event_time)`, consults the Decider, and for open positions re-consults on
  the configured cadence (e.g. once per simulated day, or when a new filing for that ticker
  arrives). It guarantees no future data is ever passed into a MarketView, and enforces the
  max-hold backstop.

- **Virtual portfolio / Paper executor** — starting cash, configurable fixed sizing per trade,
  fills at the as-of price with configurable slippage, tracks positions, realized/unrealized
  P&L, and an equity curve. Writes trades and snapshots to SQLite. (This is the Phase-0
  `Executor` swap point, realized as the replay/paper executor.)

- **Reporting** — number of trades, win rate, total/average return, equity curve, a
  model-vs-mechanical benchmark (the model's realized P&L vs. the fixed-horizon returns from
  Phase 0's `price_labels`), and the full decision/reasoning log.

## 5. Data model additions (SQLite)

- `prices` — (ticker, ts, open, high, low, close, volume); PK (ticker, ts)
- `decisions` — (id, ticker, as_of, action, conviction, reason, position_state, run_id)
- `trades` — (id, ticker, entry_ts, entry_price, exit_ts, exit_price, qty, pnl, mode, run_id);
  `mode = 'replay'` for this phase
- `portfolio_snapshots` — (run_id, ts, cash, equity)

Alongside Phase 0's `events` and `price_labels`. A `run_id` ties a replay's decisions, trades,
and snapshots together so multiple replays can coexist.

## 6. Cost & scope controls (first-run defaults, all configurable)

- Ticker set: a small watchlist (~10–20 liquid tickers)
- Window: a bounded period (e.g. the last 12 months)
- Price bars: daily
- Hold/sell cadence: once per simulated day (plus on new filings for held tickers)
- Max-hold backstop: a configurable cap (e.g. 30 trading days) that force-closes a position
- Sizing: fixed cash per trade against a starting virtual balance; no leverage

These keep the first full accuracy report cheap to produce before scaling tickers, window, or
cadence.

## 7. How no-lookahead is proven (testing)

- Unit tests assert `MarketView` returns nothing timestamped > as_of, for both events and prices.
- A test feeds a known event followed by a known future price spike and asserts the Decider's
  input contains the event but **not** the spike.
- The replay engine is tested on a tiny synthetic dataset with a deterministic stub Decider,
  verifying trades, P&L math, and the equity curve are exactly correct — offline, no API.
- `ClaudeDecider` is tested against a mocked model response, so the suite stays free and fast.
- Target coverage ≥ 80% per project standards; TDD throughout.

## 8. Out of scope (future phases)

Real-time/live forward paper trading (same engine, live clock), the live Watcher daemon,
intraday bars, news-API signals, model-driven dynamic position sizing, and any real broker
connection.

## 9. Open items to confirm in planning

- Exact first-run ticker list
- Exact replay window (start/end dates)
- Starting virtual cash and per-trade sizing
- Claude model id + max tokens for the Decider, and the structured-output schema
- Slippage assumption for fills
- Whether intraday is needed for 8-K reactions or daily bars are acceptable for v1 (default: daily)
