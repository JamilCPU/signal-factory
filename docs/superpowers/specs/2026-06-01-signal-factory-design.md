# Signal Factory — Design Spec

**Date:** 2026-06-01
**Status:** Approved design, pre-implementation
**Working name:** Signal Factory (rename freely)

---

## 1. Purpose

An automated, news- and filings-driven stock trading system. It pulls data based on
**actual business events** (SEC filings, official news wires, price/volume action) — *not*
social-media chatter — scores those events for materiality and direction, and a head agent
decides whether and how to trade a given stock.

The **end goal is fully automated trading.** The **build order deliberately climbs to that
through validation**: research/backtest → paper trading → small live → scaled live. The
execution-to-broker step is the *last* capability unlocked, and only after the system has
proven a real edge on logged results.

## 2. Decisions locked during brainstorming (2026-06-01)

| Decision | Choice |
|---|---|
| End state | Fully automated trading (destination); validation-first build order |
| Holding horizon | Undecided by design — pipeline is horizon-agnostic; **the data decides** the horizon during paper/backtest |
| Core architecture | Two tiers: deterministic **Watcher** loop + on-demand **Intelligence** |
| Modes | **Paper** (simulated, logs to DB) and **Live** (broker API) — a single Executor swap; Paper runs as a permanent shadow even in Live |
| Universe | Union of a **manual watchlist** + a **dynamic discovery feed** (tickers with unusual news volume/traction) |
| Data budget | Free public sources first; swappable behind interface; **< $100/mo** through testing |
| Signal sources | Filings / news / price only. **No social media as a trigger.** |
| Intelligence impl | **Claude Code / Claude-backed** now, behind a swappable interface for future agents |
| Runtime | Python + SQLite as a service on a Raspberry Pi (proposed Pi3 / 192.0.2.103) |

## 3. Core architecture — two tiers, three swap points

### Tier 1 — the Watcher (deterministic, always on)
Plain Python daemon. **No AI.** Polls feeds on a schedule (or listens to websockets) and
evaluates cheap yes/no conditions:
- New filing / news item since last check?
- Watchlist stock moved more than X% in the last N minutes?
- Open position crossed its stop or target?

It runs 24/7 at near-zero cost and decides **when** to summon Tier 2. This is the answer to
"how do I keep an AI on standby watching prices" — you don't; a dumb loop summons the smart
part only when something real happens.

### Tier 2 — the Intelligence (smart, summoned on demand)
Runs only when the Watcher pokes it. Reasons about a single event, returns a decision, sleeps.
Runs dozens of times a day, not millions.

### Three swap points (interfaces)
- `DataSource` — where data comes from (free APIs now, paid later)
- `Intelligence` — the agent roles (Claude now, anything later)
- `Executor` — Paper or Live

Everything else is fixed deterministic plumbing.

## 4. The pipeline — sorted by what each step actually needs

### Deterministic spine (plain Python — NOT agents)
- **Ingestor** — pulls filings/news/prices from sources
- **Deduper + Timestamper** — collapses duplicate stories; stamps the *earliest* time seen
  (critical: the signal must precede the price move, or it's "predicting" the past)
- **Price Labeler** — for every event, records what the price did after at multiple horizons
  (5min / 1hr / 1day / 1wk). This is the **truth table** that lets the system measure whether
  signals actually work
- **Backtester** — replays history against the labels with point-in-time discipline (no
  lookahead), realistic slippage and fills
- **Risk Manager** — position sizing, max loss per trade, daily circuit-breaker
- **Executor** — Paper | Live

### Intelligence (Claude-backed now, swappable)
- **Relevance Agent** — which ticker is this *really* about, and is it material to that company?
- **Materiality & Sentiment Agent** — how big a deal, which direction, how confident
- **Head Agent** — the "head stock agent": synthesizes the above *plus* current price and any
  open position, and returns: trade / don't, direction, conviction, and a written reason

> **Design note:** The user's original framing was "one agent per step, six agents + a head
> agent." We deliberately split the six steps into deterministic code vs. genuine LLM judgment.
> Deduplication, timestamping, price-labeling, and backtesting are deterministic math — an LLM
> doing them is slower, costlier, and less reliable. Only relevance, materiality/sentiment, and
> the head decision are true agent roles.

## 5. Data sources (free first, all behind `DataSource`)
- **SEC EDGAR** (free) — 8-K (material events), Form 4 (insider buys), 10-Q / 10-K
- **Alpha Vantage** (free tier) — news + sentiment, prices
- **Finnhub** (free tier) — prices, filings, fundamentals
- **Discovery feed** — surfaces tickers with unusual news volume → drops them into the active
  monitored set alongside the manual watchlist
- Upgrade path (later, if backtest justifies it): Polygon/Massive + Benzinga for low-latency,
  trader-grade news

## 6. Two modes (an Executor swap)
- **Paper (training):** writes `BUY @ market price, time T` to `trades` with `mode=paper`; the
  Watcher later logs the exit and computes P&L. No money moves.
- **Live (real):** identical inputs, calls the broker API (**Alpaca** — free, retail-friendly
  REST API).
- **Paper runs as a permanent shadow even after going live** — an ongoing report card comparing
  what the system *would* have done vs. what it actually did.

## 7. Data model (SQLite — same shape as the AI Art Factory project)
- `events` — raw, deduped, timestamped market events
- `scores` — agent outputs per event (relevance, materiality, sentiment, confidence)
- `decisions` — head-agent calls with full written reasoning
- `trades` — paper + live, with `mode` flag, entry/exit prices, P&L
- `positions` — currently open positions
- `price_labels` — what the price did after each event, per horizon (for backtest + learning)

## 8. The validation loop — how trust is earned
Every decision is logged with its full reasoning *and* its later outcome. The Backtester
replays history; Paper mode proves it going forward. Because every decision carries timestamps
and later prices, the system **measures where the edge actually lives** (1hr vs 1day vs 1week) —
that is how "let the data decide the horizon" becomes concrete.

**Promotion gate:** the Live executor stays locked until Paper mode shows positive expectancy
over a meaningful number of trades.

## 9. Build phases (the climb to automation)
- **Phase 0** — Data spine: ingest EDGAR / Alpha Vantage / Finnhub → dedupe → timestamp →
  price-label → SQLite
- **Phase 1** — Intelligence roles (Relevance, Materiality, Head), Claude-backed, behind interface
- **Phase 2** — Watcher daemon + Paper executor running 24/7, logging every decision
- **Phase 3** — Backtester + edge-by-horizon measurement
- **Phase 4** — Risk Manager + Live executor (Alpaca), tiny capital
- **Phase 5** — Scale watchlist / capital as the scoreboard earns it

## 10. Runtime
Python + SQLite, running as a service on a Raspberry Pi (proposed **Pi3 / 192.0.2.103**;
Pi1 is occupied with Pi-hole). Same architectural shape as the AI Art Factory
(collect → process → log → review), keeping the user on familiar ground.

## 11. Reality check (carried from research)
- AI's most documented edge in trading is **discipline/consistency** (removing emotional drag),
  not magical prediction. News/sentiment is a **real but moderate** edge, best as one input
  among several.
- The #1 killer is **overfitting** — strategies that win in backtest and die live. The
  point-in-time backtest + forward paper-trading gates exist specifically to catch this.
- Build order is non-negotiable: prove the signal predicts moves *before* a single real dollar
  is risked.

## 12. Open items to confirm in planning
- Final project name
- Exact Pi host (Pi3 proposed)
- Broker choice for Live phase (Alpaca proposed)
- Initial manual watchlist contents
- Concrete promotion-gate thresholds (number of paper trades + expectancy bar)
