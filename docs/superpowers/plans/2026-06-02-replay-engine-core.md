# Replay Engine Core (Phase 1a) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the offline, fully-testable historical-replay simulator: a no-lookahead `MarketView`, a swappable `Decider`, a virtual portfolio, the replay engine (model-driven entry *and* exit), result persistence, and an accuracy/P&L report — all runnable with synthetic data and a stub decider, no network or API.

**Architecture:** Extends the Phase 0 data spine. The replay engine walks events chronologically; at each decision point it builds a `MarketView(as_of)` that can only return data timestamped ≤ as_of, hands it to a `Decider`, and books fills into a `VirtualPortfolio`. Future prices used for scoring live on a separate path the `Decider` never sees. The real EDGAR/yfinance/Claude adapters are a later plan that plugs into these interfaces.

**Tech Stack:** Python 3.13, stdlib `sqlite3`, frozen `dataclasses`, `enum`, `pytest`. No new third-party dependencies.

---

## File Structure

```
src/signal_factory/
├── models.py                      # MODIFY: add Action, PriceBar, Decision, PositionState, Trade
├── storage/
│   ├── schema.sql                 # MODIFY: add prices table (Task 1); trades + decisions (Task 7)
│   ├── events_repo.py             # MODIFY: add events_for_ticker()
│   ├── prices_repo.py             # NEW: price bars CRUD + as-of queries
│   ├── trades_repo.py             # NEW: persist trades
│   └── decisions_repo.py          # NEW: persist decision log
├── market_view.py                 # NEW: MarketView + build_market_view() (the no-lookahead wall)
├── decider.py                     # NEW: Decider ABC + CallableDecider
├── portfolio.py                   # NEW: VirtualPortfolio
├── replay/
│   ├── __init__.py                # NEW
│   ├── engine.py                  # NEW: ReplayConfig, RunResult, run_replay()
│   └── persist.py                 # NEW: persist_run()
└── reporting.py                   # NEW: summarize()
tests/
├── test_models_trading.py         # NEW
├── test_prices_repo.py            # NEW
├── test_market_view.py            # NEW (no-lookahead assertions)
├── test_decider.py                # NEW
├── test_portfolio.py              # NEW
├── test_replay_engine.py          # NEW
├── test_replay_persist.py         # NEW
└── test_reporting.py              # NEW
```

---

### Task 1: Trading models + prices table

**Files:**
- Modify: `src/signal_factory/models.py`
- Modify: `src/signal_factory/storage/schema.sql`
- Test: `tests/test_models_trading.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_models_trading.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import Action, PriceBar, Decision, PositionState, Trade


def test_action_enum_values():
    assert Action.BUY.value == "BUY"
    assert Action.SELL.value == "SELL"
    assert Action.HOLD.value == "HOLD"


def test_price_bar_is_immutable():
    bar = PriceBar(ticker="AAPL", ts=datetime(2026, 6, 1, tzinfo=timezone.utc),
                   open=10.0, high=11.0, low=9.5, close=10.5, volume=1000.0)
    assert bar.close == 10.5
    try:
        bar.close = 1.0  # type: ignore[misc]
        assert False, "PriceBar should be frozen"
    except Exception:
        pass


def test_decision_holds_action_conviction_reason():
    d = Decision(action=Action.BUY, conviction=0.8, reason="positive 8-K")
    assert d.action is Action.BUY
    assert d.conviction == 0.8
    assert d.reason == "positive 8-K"


def test_position_state_open_flag():
    flat = PositionState(ticker="AAPL", qty=0.0, entry_price=0.0, entry_ts=None)
    held = PositionState(ticker="AAPL", qty=5.0, entry_price=10.0,
                         entry_ts=datetime(2026, 6, 1, tzinfo=timezone.utc))
    assert flat.is_open is False
    assert held.is_open is True


def test_trade_holds_round_trip():
    t = Trade(ticker="AAPL", entry_ts=datetime(2026, 6, 1, tzinfo=timezone.utc),
              entry_price=10.0, exit_ts=datetime(2026, 6, 2, tzinfo=timezone.utc),
              exit_price=11.0, qty=2.0, pnl=2.0)
    assert t.pnl == 2.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_models_trading.py -v`
Expected: FAIL — `ImportError: cannot import name 'Action'`.

- [ ] **Step 3: Add the models**

Append to `src/signal_factory/models.py` (keep existing content; add this below it). First add `from enum import Enum` to the imports at the top, then append:

```python
class Action(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass(frozen=True)
class PriceBar:
    ticker: str
    ts: datetime  # timezone-aware UTC
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class Decision:
    action: Action
    conviction: float
    reason: str


@dataclass(frozen=True)
class PositionState:
    ticker: str
    qty: float
    entry_price: float
    entry_ts: datetime | None

    @property
    def is_open(self) -> bool:
        return self.qty > 0.0


@dataclass(frozen=True)
class Trade:
    ticker: str
    entry_ts: datetime
    entry_price: float
    exit_ts: datetime
    exit_price: float
    qty: float
    pnl: float
```

- [ ] **Step 4: Add the prices table**

Append to `src/signal_factory/storage/schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS prices (
    ticker TEXT NOT NULL,
    ts     TEXT NOT NULL,
    open   REAL NOT NULL,
    high   REAL NOT NULL,
    low    REAL NOT NULL,
    close  REAL NOT NULL,
    volume REAL NOT NULL,
    PRIMARY KEY (ticker, ts)
);
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_models_trading.py -v`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add src/signal_factory/models.py src/signal_factory/storage/schema.sql tests/test_models_trading.py
git commit -m "feat: add trading models (Action/PriceBar/Decision/Position/Trade) and prices table"
```

---

### Task 2: Prices repository

**Files:**
- Create: `src/signal_factory/storage/prices_repo.py`
- Test: `tests/test_prices_repo.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_prices_repo.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import PriceBar
from signal_factory.storage.db import connect, init_db
from signal_factory.storage.prices_repo import (
    insert_bars, price_asof, bars_until, bars_after,
)


def _bar(day, close):
    return PriceBar(ticker="AAPL", ts=datetime(2026, 6, day, tzinfo=timezone.utc),
                    open=close, high=close, low=close, close=close, volume=100.0)


def _seed(conn):
    insert_bars(conn, [_bar(1, 100.0), _bar(2, 110.0), _bar(3, 120.0)])


def test_insert_and_price_asof_returns_latest_on_or_before():
    conn = connect(":memory:")
    init_db(conn)
    _seed(conn)
    assert price_asof(conn, "AAPL", datetime(2026, 6, 2, 12, tzinfo=timezone.utc)) == 110.0
    assert price_asof(conn, "AAPL", datetime(2026, 6, 2, tzinfo=timezone.utc)) == 110.0


def test_price_asof_none_before_first_bar():
    conn = connect(":memory:")
    init_db(conn)
    _seed(conn)
    assert price_asof(conn, "AAPL", datetime(2026, 5, 31, tzinfo=timezone.utc)) is None


def test_bars_until_includes_only_on_or_before():
    conn = connect(":memory:")
    init_db(conn)
    _seed(conn)
    bars = bars_until(conn, "AAPL", datetime(2026, 6, 2, tzinfo=timezone.utc))
    assert [b.close for b in bars] == [100.0, 110.0]


def test_bars_after_excludes_on_or_before():
    conn = connect(":memory:")
    init_db(conn)
    _seed(conn)
    bars = bars_after(conn, "AAPL", datetime(2026, 6, 1, tzinfo=timezone.utc))
    assert [b.close for b in bars] == [110.0, 120.0]


def test_insert_bars_idempotent():
    conn = connect(":memory:")
    init_db(conn)
    assert insert_bars(conn, [_bar(1, 100.0)]) == 1
    assert insert_bars(conn, [_bar(1, 100.0)]) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_prices_repo.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.storage.prices_repo'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/signal_factory/storage/prices_repo.py`:

```python
from __future__ import annotations

import sqlite3
from datetime import datetime

from signal_factory.models import PriceBar


def _row_to_bar(row: sqlite3.Row) -> PriceBar:
    return PriceBar(
        ticker=row["ticker"],
        ts=datetime.fromisoformat(row["ts"]),
        open=row["open"], high=row["high"], low=row["low"],
        close=row["close"], volume=row["volume"],
    )


def insert_bars(conn: sqlite3.Connection, bars: list[PriceBar]) -> int:
    """Insert price bars, ignoring duplicates by (ticker, ts). Returns rows added."""
    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO prices (ticker, ts, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (b.ticker, b.ts.isoformat(), b.open, b.high, b.low, b.close, b.volume)
            for b in bars
        ],
    )
    conn.commit()
    return conn.total_changes - before


def price_asof(conn: sqlite3.Connection, ticker: str, when: datetime) -> float | None:
    """Most recent close with ts <= when, or None."""
    row = conn.execute(
        "SELECT close FROM prices WHERE ticker = ? AND ts <= ? ORDER BY ts DESC LIMIT 1",
        (ticker, when.isoformat()),
    ).fetchone()
    return row["close"] if row else None


def bars_until(conn: sqlite3.Connection, ticker: str, when: datetime) -> list[PriceBar]:
    """All bars with ts <= when, ascending."""
    rows = conn.execute(
        "SELECT * FROM prices WHERE ticker = ? AND ts <= ? ORDER BY ts",
        (ticker, when.isoformat()),
    ).fetchall()
    return [_row_to_bar(r) for r in rows]


def bars_after(conn: sqlite3.Connection, ticker: str, when: datetime) -> list[PriceBar]:
    """All bars with ts > when, ascending."""
    rows = conn.execute(
        "SELECT * FROM prices WHERE ticker = ? AND ts > ? ORDER BY ts",
        (ticker, when.isoformat()),
    ).fetchall()
    return [_row_to_bar(r) for r in rows]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_prices_repo.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/storage/prices_repo.py tests/test_prices_repo.py
git commit -m "feat: add prices repository with as-of queries"
```

---

### Task 3: MarketView — the no-lookahead wall

**Files:**
- Create: `src/signal_factory/market_view.py`
- Modify: `src/signal_factory/storage/events_repo.py`
- Test: `tests/test_market_view.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_market_view.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import make_event, PriceBar
from signal_factory.market_view import build_market_view
from signal_factory.storage.db import connect, init_db
from signal_factory.storage.events_repo import insert_events
from signal_factory.storage.prices_repo import insert_bars


def _event(minute, title):
    return make_event(
        source="edgar", event_type="8-K", ticker="AAPL", title=title, body="b",
        url="u", published_at=datetime(2026, 6, 1, 14, minute, tzinfo=timezone.utc),
    )


def _bar(day, close):
    return PriceBar(ticker="AAPL", ts=datetime(2026, 6, day, tzinfo=timezone.utc),
                    open=close, high=close, low=close, close=close, volume=1.0)


def test_market_view_excludes_future_events():
    conn = connect(":memory:")
    init_db(conn)
    past = _event(0, "past filing")
    future = _event(59, "future filing")
    insert_events(conn, [past, future])
    as_of = datetime(2026, 6, 1, 14, 30, tzinfo=timezone.utc)

    view = build_market_view(conn, as_of, "AAPL", trigger=past)

    titles = [e.title for e in view.prior_events]
    assert "past filing" in titles
    assert "future filing" not in titles  # NO LOOKAHEAD


def test_market_view_excludes_future_prices():
    conn = connect(":memory:")
    init_db(conn)
    insert_bars(conn, [_bar(1, 100.0), _bar(2, 110.0), _bar(3, 999.0)])  # day 3 is the future spike
    as_of = datetime(2026, 6, 2, 23, tzinfo=timezone.utc)

    view = build_market_view(conn, as_of, "AAPL", trigger=None)

    closes = [b.close for b in view.price_history]
    assert closes == [100.0, 110.0]
    assert 999.0 not in closes  # the future spike is walled off
    assert view.latest_price() == 110.0


def test_market_view_carries_trigger_and_as_of():
    conn = connect(":memory:")
    init_db(conn)
    trig = _event(0, "trigger filing")
    insert_events(conn, [trig])
    as_of = datetime(2026, 6, 1, 14, 30, tzinfo=timezone.utc)
    view = build_market_view(conn, as_of, "AAPL", trigger=trig)
    assert view.as_of == as_of
    assert view.ticker == "AAPL"
    assert view.trigger == trig
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_market_view.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.market_view'`.

- [ ] **Step 3: Add events_for_ticker to the events repo**

Append to `src/signal_factory/storage/events_repo.py`:

```python
def events_for_ticker(conn: sqlite3.Connection, ticker: str) -> list[Event]:
    rows = conn.execute(
        "SELECT * FROM events WHERE ticker = ? ORDER BY published_at", (ticker,)
    ).fetchall()
    return [_row_to_event(r) for r in rows]
```

- [ ] **Step 4: Write the MarketView**

Create `src/signal_factory/market_view.py`:

```python
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime

from signal_factory.models import Event, PriceBar
from signal_factory.storage.events_repo import events_for_ticker
from signal_factory.storage.prices_repo import bars_until


@dataclass(frozen=True)
class MarketView:
    """The ONLY data window a Decider sees. By construction it contains nothing
    timestamped after `as_of` — this is the no-lookahead wall."""

    as_of: datetime
    ticker: str
    trigger: Event | None
    prior_events: list[Event]
    price_history: list[PriceBar]

    def latest_price(self) -> float | None:
        return self.price_history[-1].close if self.price_history else None


def build_market_view(
    conn: sqlite3.Connection,
    as_of: datetime,
    ticker: str,
    trigger: Event | None,
) -> MarketView:
    """Build an as-of view: only events and price bars with timestamp <= as_of."""
    events = [e for e in events_for_ticker(conn, ticker) if e.published_at <= as_of]
    prices = bars_until(conn, ticker, as_of)
    return MarketView(
        as_of=as_of,
        ticker=ticker,
        trigger=trigger,
        prior_events=events,
        price_history=prices,
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_market_view.py -v`
Expected: PASS (3 tests, including the two no-lookahead assertions).

- [ ] **Step 6: Commit**

```bash
git add src/signal_factory/market_view.py src/signal_factory/storage/events_repo.py tests/test_market_view.py
git commit -m "feat: add no-lookahead MarketView and events_for_ticker query"
```

---

### Task 4: Decider interface

**Files:**
- Create: `src/signal_factory/decider.py`
- Test: `tests/test_decider.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_decider.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import Action, Decision, PositionState
from signal_factory.market_view import MarketView
from signal_factory.decider import Decider, CallableDecider


def _view():
    return MarketView(as_of=datetime(2026, 6, 1, tzinfo=timezone.utc), ticker="AAPL",
                      trigger=None, prior_events=[], price_history=[])


def test_callable_decider_delegates_to_function():
    def always_buy(view, position):
        return Decision(action=Action.BUY, conviction=1.0, reason="test")

    decider = CallableDecider(always_buy)
    assert isinstance(decider, Decider)
    flat = PositionState("AAPL", 0.0, 0.0, None)
    decision = decider.decide(_view(), flat)
    assert decision.action is Action.BUY
    assert decision.reason == "test"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_decider.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.decider'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/signal_factory/decider.py`:

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable

from signal_factory.models import Decision, PositionState
from signal_factory.market_view import MarketView


class Decider(ABC):
    """Decides what to do given an as-of MarketView and the current position.
    The real implementation calls the Claude model; the interface keeps it
    swappable and lets tests inject deterministic logic."""

    @abstractmethod
    def decide(self, view: MarketView, position: PositionState) -> Decision:
        raise NotImplementedError


class CallableDecider(Decider):
    """Wraps a plain function as a Decider — used for deterministic tests and
    simple baseline strategies."""

    def __init__(self, fn: Callable[[MarketView, PositionState], Decision]) -> None:
        self._fn = fn

    def decide(self, view: MarketView, position: PositionState) -> Decision:
        return self._fn(view, position)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_decider.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/decider.py tests/test_decider.py
git commit -m "feat: add Decider interface and CallableDecider"
```

---

### Task 5: Virtual portfolio

**Files:**
- Create: `src/signal_factory/portfolio.py`
- Test: `tests/test_portfolio.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_portfolio.py`:

```python
from datetime import datetime, timezone

from signal_factory.portfolio import VirtualPortfolio


T1 = datetime(2026, 6, 1, tzinfo=timezone.utc)
T2 = datetime(2026, 6, 2, tzinfo=timezone.utc)


def test_buy_opens_position_and_spends_cash():
    p = VirtualPortfolio(starting_cash=1000.0)
    p.buy("AAPL", price=100.0, ts=T1, cash_amount=500.0)
    pos = p.position("AAPL")
    assert pos.is_open
    assert pos.qty == 5.0          # 500 / 100
    assert pos.entry_price == 100.0
    assert p.cash == 500.0


def test_buy_ignored_when_already_open():
    p = VirtualPortfolio(starting_cash=1000.0)
    p.buy("AAPL", price=100.0, ts=T1, cash_amount=500.0)
    p.buy("AAPL", price=100.0, ts=T1, cash_amount=500.0)  # second buy ignored
    assert p.cash == 500.0
    assert p.position("AAPL").qty == 5.0


def test_sell_closes_position_and_realizes_profit():
    p = VirtualPortfolio(starting_cash=1000.0)
    p.buy("AAPL", price=100.0, ts=T1, cash_amount=500.0)
    trade = p.sell("AAPL", price=120.0, ts=T2)
    assert trade is not None
    assert trade.pnl == 100.0       # (120 - 100) * 5
    assert p.cash == 1100.0         # 500 left + 600 proceeds
    assert p.position("AAPL").is_open is False
    assert p.trades == [trade]


def test_sell_with_no_position_returns_none():
    p = VirtualPortfolio(starting_cash=1000.0)
    assert p.sell("AAPL", price=120.0, ts=T2) is None


def test_equity_marks_open_positions_to_market():
    p = VirtualPortfolio(starting_cash=1000.0)
    p.buy("AAPL", price=100.0, ts=T1, cash_amount=500.0)  # 5 shares, 500 cash left
    assert p.equity({"AAPL": 100.0}) == 1000.0
    assert p.equity({"AAPL": 120.0}) == 1100.0            # 500 + 5*120
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_portfolio.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.portfolio'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/signal_factory/portfolio.py`:

```python
from __future__ import annotations

from datetime import datetime

from signal_factory.models import PositionState, Trade


class VirtualPortfolio:
    """A simple long-only paper portfolio: cash, one position per ticker,
    fractional shares, fills at the price passed in (slippage is applied by the
    caller). Realized P&L is booked on sell."""

    def __init__(self, starting_cash: float) -> None:
        self.cash = starting_cash
        self.positions: dict[str, PositionState] = {}
        self.trades: list[Trade] = []

    def position(self, ticker: str) -> PositionState:
        return self.positions.get(
            ticker, PositionState(ticker=ticker, qty=0.0, entry_price=0.0, entry_ts=None)
        )

    def buy(self, ticker: str, price: float, ts: datetime, cash_amount: float) -> None:
        if price <= 0 or self.position(ticker).is_open:
            return
        spend = min(cash_amount, self.cash)
        if spend <= 0:
            return
        qty = spend / price
        self.cash -= spend
        self.positions[ticker] = PositionState(
            ticker=ticker, qty=qty, entry_price=price, entry_ts=ts
        )

    def sell(self, ticker: str, price: float, ts: datetime) -> Trade | None:
        pos = self.position(ticker)
        if not pos.is_open:
            return None
        self.cash += pos.qty * price
        pnl = (price - pos.entry_price) * pos.qty
        trade = Trade(
            ticker=ticker, entry_ts=pos.entry_ts, entry_price=pos.entry_price,
            exit_ts=ts, exit_price=price, qty=pos.qty, pnl=pnl,
        )
        self.trades.append(trade)
        self.positions[ticker] = PositionState(
            ticker=ticker, qty=0.0, entry_price=0.0, entry_ts=None
        )
        return trade

    def equity(self, mark_prices: dict[str, float]) -> float:
        total = self.cash
        for ticker, pos in self.positions.items():
            if pos.is_open:
                total += pos.qty * mark_prices.get(ticker, pos.entry_price)
        return total
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_portfolio.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/portfolio.py tests/test_portfolio.py
git commit -m "feat: add virtual paper portfolio with realized P&L"
```

---

### Task 6: Replay engine

**Files:**
- Create: `src/signal_factory/replay/__init__.py`
- Create: `src/signal_factory/replay/engine.py`
- Test: `tests/test_replay_engine.py`

The engine drives the whole simulation: walk events in time order; for a flat ticker, ask the
Decider on the triggering event; on a BUY, manage the open position bar-by-bar (the daily
cadence) until the Decider SELLs or `max_hold_bars` is hit. Every `MarketView` is built at the
current as_of, so the Decider never sees the future. Slippage is applied to fills here, keeping
the portfolio pure.

- [ ] **Step 1: Write the failing test**

Create `tests/test_replay_engine.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import Action, Decision, make_event, PriceBar
from signal_factory.decider import CallableDecider
from signal_factory.replay.engine import ReplayConfig, run_replay
from signal_factory.storage.db import connect, init_db
from signal_factory.storage.events_repo import insert_events
from signal_factory.storage.prices_repo import insert_bars


def _event(day, ticker="AAPL"):
    return make_event(
        source="edgar", event_type="8-K", ticker=ticker, title=f"{ticker} 8-K day {day}",
        body="b", url="u",
        published_at=datetime(2026, 6, day, 14, 0, tzinfo=timezone.utc),
    )


def _bar(day, close, ticker="AAPL"):
    return PriceBar(ticker=ticker, ts=datetime(2026, 6, day, 21, 0, tzinfo=timezone.utc),
                    open=close, high=close, low=close, close=close, volume=1.0)


def _seed(conn):
    # Event on day 1; prices rise 100 -> 100 (entry) -> 110 -> 121
    insert_events(conn, [_event(1)])
    insert_bars(conn, [_bar(1, 100.0), _bar(2, 110.0), _bar(3, 121.0)])


def test_buy_on_event_then_sell_when_up_10pct():
    conn = connect(":memory:")
    init_db(conn)
    _seed(conn)

    def strategy(view, position):
        if not position.is_open:
            return Decision(Action.BUY, 1.0, "enter on 8-K")
        gain = (view.latest_price() - position.entry_price) / position.entry_price
        if gain >= 0.10:
            return Decision(Action.SELL, 1.0, "take profit")
        return Decision(Action.HOLD, 0.5, "wait")

    config = ReplayConfig(cash_per_trade=1000.0, max_hold_bars=10, slippage_pct=0.0)
    result = run_replay(conn, [_event(1)], CallableDecider(strategy),
                        config, starting_cash=1000.0)

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.entry_price == 100.0
    assert trade.exit_price == 110.0          # first bar with >=10% gain
    assert round(trade.pnl, 6) == 100.0       # (110-100) * (1000/100)
    assert round(result.ending_cash, 6) == 1100.0


def test_max_hold_forces_exit_at_last_bar():
    conn = connect(":memory:")
    init_db(conn)
    insert_events(conn, [_event(1)])
    insert_bars(conn, [_bar(1, 100.0), _bar(2, 101.0), _bar(3, 102.0)])

    def buy_and_never_sell(view, position):
        if not position.is_open:
            return Decision(Action.BUY, 1.0, "enter")
        return Decision(Action.HOLD, 0.5, "hold forever")

    config = ReplayConfig(cash_per_trade=1000.0, max_hold_bars=2, slippage_pct=0.0)
    result = run_replay(conn, [_event(1)], CallableDecider(buy_and_never_sell),
                        config, starting_cash=1000.0)

    assert len(result.trades) == 1
    # only 2 bars after entry are considered; forced exit on the 2nd (day 3, close 102)
    assert result.trades[0].exit_price == 102.0


def test_slippage_worsens_fills():
    conn = connect(":memory:")
    init_db(conn)
    _seed(conn)

    def strategy(view, position):
        if not position.is_open:
            return Decision(Action.BUY, 1.0, "enter")
        gain = (view.latest_price() - position.entry_price) / position.entry_price
        return Decision(Action.SELL, 1.0, "exit") if gain >= 0.10 else Decision(Action.HOLD, 0.5, "")

    config = ReplayConfig(cash_per_trade=1000.0, max_hold_bars=10, slippage_pct=0.01)
    result = run_replay(conn, [_event(1)], CallableDecider(strategy),
                        config, starting_cash=1000.0)

    trade = result.trades[0]
    assert trade.entry_price == 101.0   # 100 * (1 + 0.01) buy slippage
    assert trade.exit_price == 108.9    # 110 * (1 - 0.01) sell slippage


def test_no_buy_means_no_trades():
    conn = connect(":memory:")
    init_db(conn)
    _seed(conn)

    def never(view, position):
        return Decision(Action.HOLD, 0.0, "skip")

    config = ReplayConfig(cash_per_trade=1000.0, max_hold_bars=10, slippage_pct=0.0)
    result = run_replay(conn, [_event(1)], CallableDecider(never),
                        config, starting_cash=1000.0)
    assert result.trades == []
    assert result.ending_cash == 1000.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_replay_engine.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.replay'`.

- [ ] **Step 3: Write minimal implementation**

Create empty `src/signal_factory/replay/__init__.py`.

Create `src/signal_factory/replay/engine.py`:

```python
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime

from signal_factory.decider import Decider
from signal_factory.market_view import build_market_view
from signal_factory.models import Action, Decision, Event
from signal_factory.portfolio import VirtualPortfolio
from signal_factory.storage.prices_repo import bars_after, price_asof


@dataclass(frozen=True)
class ReplayConfig:
    cash_per_trade: float
    max_hold_bars: int
    slippage_pct: float = 0.0


@dataclass
class RunResult:
    trades: list = field(default_factory=list)
    decisions: list = field(default_factory=list)  # (ticker, as_of, Decision)
    starting_cash: float = 0.0
    ending_cash: float = 0.0

    @property
    def realized_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)


def _fill_price(price: float, action: Action, slippage_pct: float) -> float:
    if action is Action.BUY:
        return price * (1 + slippage_pct)
    return price * (1 - slippage_pct)  # SELL


def _manage_open_position(
    conn: sqlite3.Connection,
    portfolio: VirtualPortfolio,
    decider: Decider,
    config: ReplayConfig,
    ticker: str,
    decisions: list,
) -> None:
    """Walk the daily bars after entry, asking the Decider each bar, until it
    SELLs or max_hold_bars is reached (forced exit on the last considered bar)."""
    entry_ts = portfolio.position(ticker).entry_ts
    bars = bars_after(conn, ticker, entry_ts)[: config.max_hold_bars]
    for i, bar in enumerate(bars):
        view = build_market_view(conn, bar.ts, ticker, trigger=None)
        decision = decider.decide(view, portfolio.position(ticker))
        decisions.append((ticker, bar.ts, decision))
        is_last = i == len(bars) - 1
        if decision.action is Action.SELL or is_last:
            portfolio.sell(ticker, _fill_price(bar.close, Action.SELL, config.slippage_pct), bar.ts)
            return


def run_replay(
    conn: sqlite3.Connection,
    events: list[Event],
    decider: Decider,
    config: ReplayConfig,
    starting_cash: float,
) -> RunResult:
    """Replay events chronologically with strict no-lookahead. Model-driven entry
    and exit. Returns the trades, the full decision log, and ending cash."""
    portfolio = VirtualPortfolio(starting_cash)
    decisions: list = []

    for event in sorted(events, key=lambda e: e.published_at):
        ticker = event.ticker
        as_of = event.published_at
        if portfolio.position(ticker).is_open:
            continue  # already holding this ticker; managed via its own bar cadence

        view = build_market_view(conn, as_of, ticker, trigger=event)
        decision = decider.decide(view, portfolio.position(ticker))
        decisions.append((ticker, as_of, decision))

        if decision.action is Action.BUY:
            base = view.latest_price()
            if base is None:
                base = price_asof(conn, ticker, as_of)
            if base is not None:
                portfolio.buy(ticker, _fill_price(base, Action.BUY, config.slippage_pct),
                              as_of, config.cash_per_trade)
                if portfolio.position(ticker).is_open:
                    _manage_open_position(conn, portfolio, decider, config, ticker, decisions)

    return RunResult(
        trades=list(portfolio.trades),
        decisions=decisions,
        starting_cash=starting_cash,
        ending_cash=portfolio.cash,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_replay_engine.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/replay/__init__.py src/signal_factory/replay/engine.py tests/test_replay_engine.py
git commit -m "feat: add no-lookahead replay engine with model-driven entry and exit"
```

---

### Task 7: Persist run results

**Files:**
- Modify: `src/signal_factory/storage/schema.sql`
- Create: `src/signal_factory/storage/trades_repo.py`
- Create: `src/signal_factory/storage/decisions_repo.py`
- Create: `src/signal_factory/replay/persist.py`
- Test: `tests/test_replay_persist.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_replay_persist.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import Action, Decision, Trade
from signal_factory.replay.engine import RunResult
from signal_factory.replay.persist import persist_run
from signal_factory.storage.db import connect, init_db
from signal_factory.storage.trades_repo import trades_for_run
from signal_factory.storage.decisions_repo import decisions_for_run


def _result():
    trade = Trade(ticker="AAPL", entry_ts=datetime(2026, 6, 1, tzinfo=timezone.utc),
                  entry_price=100.0, exit_ts=datetime(2026, 6, 2, tzinfo=timezone.utc),
                  exit_price=110.0, qty=5.0, pnl=50.0)
    decisions = [("AAPL", datetime(2026, 6, 1, tzinfo=timezone.utc),
                  Decision(Action.BUY, 0.9, "enter"))]
    return RunResult(trades=[trade], decisions=decisions,
                     starting_cash=1000.0, ending_cash=1050.0)


def test_persist_run_writes_trades_and_decisions():
    conn = connect(":memory:")
    init_db(conn)
    persist_run(conn, "run-1", _result())

    trades = trades_for_run(conn, "run-1")
    assert len(trades) == 1
    assert trades[0].pnl == 50.0

    decisions = decisions_for_run(conn, "run-1")
    assert len(decisions) == 1
    assert decisions[0]["action"] == "BUY"
    assert decisions[0]["reason"] == "enter"


def test_runs_are_isolated_by_run_id():
    conn = connect(":memory:")
    init_db(conn)
    persist_run(conn, "run-1", _result())
    persist_run(conn, "run-2", _result())
    assert len(trades_for_run(conn, "run-1")) == 1
    assert len(trades_for_run(conn, "run-2")) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_replay_persist.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.replay.persist'`.

- [ ] **Step 3: Add the tables**

Append to `src/signal_factory/storage/schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS trades (
    run_id      TEXT NOT NULL,
    ticker      TEXT NOT NULL,
    entry_ts    TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_ts     TEXT NOT NULL,
    exit_price  REAL NOT NULL,
    qty         REAL NOT NULL,
    pnl         REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_run ON trades(run_id);

CREATE TABLE IF NOT EXISTS decisions (
    run_id     TEXT NOT NULL,
    ticker     TEXT NOT NULL,
    as_of      TEXT NOT NULL,
    action     TEXT NOT NULL,
    conviction REAL NOT NULL,
    reason     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_run ON decisions(run_id);
```

- [ ] **Step 4: Write the repos**

Create `src/signal_factory/storage/trades_repo.py`:

```python
from __future__ import annotations

import sqlite3
from datetime import datetime

from signal_factory.models import Trade


def insert_trades(conn: sqlite3.Connection, run_id: str, trades: list[Trade]) -> int:
    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO trades
            (run_id, ticker, entry_ts, entry_price, exit_ts, exit_price, qty, pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (run_id, t.ticker, t.entry_ts.isoformat(), t.entry_price,
             t.exit_ts.isoformat(), t.exit_price, t.qty, t.pnl)
            for t in trades
        ],
    )
    conn.commit()
    return conn.total_changes - before


def trades_for_run(conn: sqlite3.Connection, run_id: str) -> list[Trade]:
    rows = conn.execute(
        "SELECT * FROM trades WHERE run_id = ? ORDER BY entry_ts", (run_id,)
    ).fetchall()
    return [
        Trade(
            ticker=r["ticker"],
            entry_ts=datetime.fromisoformat(r["entry_ts"]),
            entry_price=r["entry_price"],
            exit_ts=datetime.fromisoformat(r["exit_ts"]),
            exit_price=r["exit_price"],
            qty=r["qty"],
            pnl=r["pnl"],
        )
        for r in rows
    ]
```

Create `src/signal_factory/storage/decisions_repo.py`:

```python
from __future__ import annotations

import sqlite3


def insert_decisions(conn: sqlite3.Connection, run_id: str, decisions: list) -> int:
    """decisions: list of (ticker, as_of: datetime, Decision)."""
    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO decisions (run_id, ticker, as_of, action, conviction, reason)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (run_id, ticker, as_of.isoformat(), decision.action.value,
             decision.conviction, decision.reason)
            for ticker, as_of, decision in decisions
        ],
    )
    conn.commit()
    return conn.total_changes - before


def decisions_for_run(conn: sqlite3.Connection, run_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM decisions WHERE run_id = ? ORDER BY as_of", (run_id,)
    ).fetchall()
    return [
        {
            "ticker": r["ticker"],
            "as_of": r["as_of"],
            "action": r["action"],
            "conviction": r["conviction"],
            "reason": r["reason"],
        }
        for r in rows
    ]
```

Create `src/signal_factory/replay/persist.py`:

```python
from __future__ import annotations

import sqlite3

from signal_factory.replay.engine import RunResult
from signal_factory.storage.trades_repo import insert_trades
from signal_factory.storage.decisions_repo import insert_decisions


def persist_run(conn: sqlite3.Connection, run_id: str, result: RunResult) -> None:
    """Write a replay run's trades and decision log to the database."""
    insert_trades(conn, run_id, result.trades)
    insert_decisions(conn, run_id, result.decisions)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_replay_persist.py -v`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/signal_factory/storage/schema.sql src/signal_factory/storage/trades_repo.py src/signal_factory/storage/decisions_repo.py src/signal_factory/replay/persist.py tests/test_replay_persist.py
git commit -m "feat: persist replay trades and decision log by run_id"
```

---

### Task 8: Reporting + end-to-end synthetic replay

**Files:**
- Create: `src/signal_factory/reporting.py`
- Test: `tests/test_reporting.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_reporting.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import Action, Decision, make_event, PriceBar
from signal_factory.decider import CallableDecider
from signal_factory.replay.engine import ReplayConfig, run_replay
from signal_factory.reporting import summarize
from signal_factory.storage.db import connect, init_db
from signal_factory.storage.events_repo import insert_events
from signal_factory.storage.prices_repo import insert_bars


def test_summarize_computes_winrate_and_returns():
    from signal_factory.models import Trade
    from signal_factory.replay.engine import RunResult
    winner = Trade("AAPL", datetime(2026, 6, 1, tzinfo=timezone.utc), 100.0,
                   datetime(2026, 6, 2, tzinfo=timezone.utc), 110.0, 1.0, 10.0)
    loser = Trade("MSFT", datetime(2026, 6, 1, tzinfo=timezone.utc), 100.0,
                  datetime(2026, 6, 2, tzinfo=timezone.utc), 95.0, 1.0, -5.0)
    result = RunResult(trades=[winner, loser], decisions=[],
                       starting_cash=1000.0, ending_cash=1005.0)

    report = summarize(result)
    assert report["trades"] == 2
    assert report["wins"] == 1
    assert report["win_rate"] == 0.5
    assert report["total_pnl"] == 5.0
    assert round(report["return_pct"], 4) == 0.5  # (1005-1000)/1000*100


def test_summarize_empty_run():
    from signal_factory.replay.engine import RunResult
    report = summarize(RunResult(trades=[], decisions=[], starting_cash=1000.0,
                                 ending_cash=1000.0))
    assert report["trades"] == 0
    assert report["win_rate"] == 0.0
    assert report["return_pct"] == 0.0


def test_end_to_end_replay_produces_profitable_report():
    conn = connect(":memory:")
    init_db(conn)
    insert_events(conn, [make_event(
        source="edgar", event_type="8-K", ticker="AAPL", title="strong 8-K",
        body="b", url="u",
        published_at=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc))])
    insert_bars(conn, [
        PriceBar("AAPL", datetime(2026, 6, d, 21, 0, tzinfo=timezone.utc),
                 c, c, c, c, 1.0)
        for d, c in [(1, 100.0), (2, 105.0), (3, 112.0)]
    ])

    def strategy(view, position):
        if not position.is_open:
            return Decision(Action.BUY, 1.0, "enter on 8-K")
        gain = (view.latest_price() - position.entry_price) / position.entry_price
        if gain >= 0.10:
            return Decision(Action.SELL, 1.0, "take profit")
        return Decision(Action.HOLD, 0.5, "hold")

    events = [make_event(source="edgar", event_type="8-K", ticker="AAPL",
                         title="strong 8-K", body="b", url="u",
                         published_at=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc))]
    config = ReplayConfig(cash_per_trade=1000.0, max_hold_bars=10, slippage_pct=0.0)
    result = run_replay(conn, events, CallableDecider(strategy), config, starting_cash=1000.0)

    report = summarize(result)
    assert report["trades"] == 1
    assert report["win_rate"] == 1.0
    assert report["total_pnl"] > 0
    # entry 100, exit 112 (first bar >=10%): pnl = (112-100)*10 = 120
    assert round(report["total_pnl"], 6) == 120.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_reporting.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.reporting'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/signal_factory/reporting.py`:

```python
from __future__ import annotations

from signal_factory.replay.engine import RunResult


def summarize(result: RunResult) -> dict:
    """Aggregate a replay run into headline accuracy/P&L metrics."""
    trades = result.trades
    n = len(trades)
    wins = sum(1 for t in trades if t.pnl > 0)
    total_pnl = sum(t.pnl for t in trades)
    start = result.starting_cash
    return {
        "trades": n,
        "wins": wins,
        "win_rate": (wins / n) if n else 0.0,
        "total_pnl": total_pnl,
        "avg_pnl": (total_pnl / n) if n else 0.0,
        "return_pct": ((result.ending_cash - start) / start * 100.0) if start else 0.0,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_reporting.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: PASS — all Phase 0 + Phase 1a tests green.

- [ ] **Step 6: Commit**

```bash
git add src/signal_factory/reporting.py tests/test_reporting.py
git commit -m "feat: add replay run reporting and end-to-end synthetic replay test"
```

---

## Definition of Done (Phase 1a)

- `python -m pytest -q` fully green.
- A complete replay runs end-to-end on synthetic events + bars with a stub `Decider`, with
  model-driven entry and exit, producing trades, a persisted decision log, and a P&L/accuracy
  report — entirely offline.
- No-lookahead is enforced by `MarketView` and proven by `tests/test_market_view.py`.

## What Phase 1a deliberately excludes (Plan 1b)

- `EdgarHistorySource` (real filings), `PriceHistoryLoader` (real yfinance/Stooq bars), and
  `ClaudeDecider` (the real model) — these need network/API and slot into the `DataSource`,
  prices table, and `Decider` interfaces built here.
- Live forward paper trading, the Watcher daemon, intraday bars, and the model-vs-mechanical
  benchmark column (needs Phase 0 `price_labels` joined into the report).

---

## Self-Review

**Spec coverage:** No-lookahead wall (spec §3) → Task 3 `MarketView` + assertions. Components
(§4): historical ingestors → deferred to Plan 1b (documented); `MarketView` → Task 3; `Decider`
+ Claude → interface in Task 4, real Claude in 1b; replay engine → Task 6; virtual portfolio →
Task 5; reporting → Task 8. Data model (§5): `prices` → Task 1; `trades`/`decisions` → Task 7;
`portfolio_snapshots` → not needed for 1a (equity is computed/returned; deferred). Cost/scope
(§6) → `ReplayConfig` (cash_per_trade, max_hold_bars, slippage_pct) in Task 6; ticker/window
selection belongs to Plan 1b's ingestion. Testing (§7): no-lookahead tests (Task 3), synthetic
engine test (Task 6), end-to-end (Task 8); `ClaudeDecider` mock test → Plan 1b.

**Placeholder scan:** None — every code step is complete; every run step has command + expected
result.

**Type consistency:** `Action`, `PriceBar`, `Decision`, `PositionState`, `Trade` (Task 1) used
identically in Tasks 2–8. `MarketView` fields (`as_of`, `ticker`, `trigger`, `prior_events`,
`price_history`, `latest_price()`) consistent across Tasks 3, 4, 6. `Decider.decide(view,
position)` signature consistent (Tasks 4, 6). `VirtualPortfolio.buy/sell/position/equity` (Task
5) match engine calls (Task 6). `ReplayConfig(cash_per_trade, max_hold_bars, slippage_pct)` and
`RunResult(trades, decisions, starting_cash, ending_cash, realized_pnl)` consistent across Tasks
6, 7, 8. `persist_run`/`trades_for_run`/`decisions_for_run` consistent (Task 7). Prices repo
funcs `insert_bars`/`price_asof`/`bars_until`/`bars_after` consistent (Tasks 2, 3, 6).
