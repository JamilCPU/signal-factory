# Signal Factory — Phase 0: Data Spine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the deterministic data spine that ingests market events, removes duplicates, normalizes timestamps, labels each event with what the price did afterward, and stores it all in SQLite.

**Architecture:** Pure-Python, dependency-light pipeline. Events flow `DataSource → deduplicate → store → label → store`. Every external boundary (data feeds, price history) sits behind an interface so the core logic is fully testable offline with fixtures and fakes — no API keys, no network. Real API adapters (EDGAR, Alpha Vantage, Finnhub) are a later plan; this plan ships the spine and a fixture-backed source.

**Tech Stack:** Python 3.13, standard-library `sqlite3` (no ORM — YAGNI), frozen `dataclasses` for immutable models, `hashlib` for content hashing, `pytest` for tests.

---

## File Structure

```
signal-factory/
├── pyproject.toml                       # project + pytest config (src on pythonpath)
├── .gitignore
├── src/signal_factory/
│   ├── __init__.py
│   ├── models.py                        # Event, PriceLabel, hashing helpers
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── schema.sql                   # SQLite DDL
│   │   ├── db.py                        # connect() + init_db()
│   │   ├── events_repo.py               # insert/query events
│   │   └── labels_repo.py               # insert/query price labels
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── base.py                      # DataSource interface (ABC)
│   │   └── fixture.py                   # FixtureDataSource (reads canned JSON)
│   └── pipeline/
│       ├── __init__.py
│       ├── dedupe.py                    # deduplicate()
│       ├── labeler.py                   # PriceProvider, HORIZONS, label_event()
│       └── runner.py                    # end-to-end run_ingest()
└── tests/
    ├── __init__.py
    ├── fixtures/events_sample.json
    ├── test_models.py
    ├── test_storage.py
    ├── test_dedupe.py
    ├── test_sources.py
    ├── test_labeler.py
    ├── test_labels_repo.py
    └── test_runner.py
```

Each module has one responsibility. Files that change together (a repo and the model it persists) stay close. Interfaces (`DataSource`, `PriceProvider`) isolate the testable core from the messy outside world.

---

### Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `src/signal_factory/__init__.py`
- Create: `tests/__init__.py`
- Test: `tests/test_smoke.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_smoke.py`:

```python
import signal_factory


def test_package_imports():
    assert signal_factory.__version__ == "0.0.0"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_smoke.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory'` (package/config not present yet).

- [ ] **Step 3: Create project config and package**

Create `pyproject.toml`:

```toml
[project]
name = "signal-factory"
version = "0.0.0"
description = "News- and filings-driven stock signal engine"
requires-python = ">=3.11"

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

Create `.gitignore`:

```gitignore
__pycache__/
*.pyc
.pytest_cache/
*.db
*.sqlite
.venv/
venv/
.env
```

Create `src/signal_factory/__init__.py`:

```python
__version__ = "0.0.0"
```

Create empty `tests/__init__.py` (no content).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_smoke.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml .gitignore src/signal_factory/__init__.py tests/__init__.py tests/test_smoke.py
git commit -m "chore: scaffold signal-factory package and pytest config"
```

---

### Task 2: Data models and content hashing

**Files:**
- Create: `src/signal_factory/models.py`
- Test: `tests/test_models.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_models.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import (
    Event,
    PriceLabel,
    normalize_text,
    compute_content_hash,
    make_event,
)


def test_normalize_text_lowercases_and_collapses_whitespace():
    assert normalize_text("  Apple   Beats\n Earnings ") == "apple beats earnings"


def test_content_hash_is_stable_and_ignores_formatting():
    a = compute_content_hash("Apple Beats", "Body text")
    b = compute_content_hash("  apple   beats ", "body   text")
    assert a == b
    assert len(a) == 64  # sha256 hex


def test_content_hash_differs_on_different_content():
    assert compute_content_hash("Apple up", "x") != compute_content_hash("Apple down", "x")


def test_make_event_computes_hash_and_uppercases_ticker():
    ts = datetime(2026, 6, 1, 14, 30, tzinfo=timezone.utc)
    ev = make_event(
        source="edgar",
        event_type="8-K",
        ticker="aapl",
        title="Apple files 8-K",
        body="Material event.",
        url="https://example.com/1",
        published_at=ts,
    )
    assert isinstance(ev, Event)
    assert ev.ticker == "AAPL"
    assert ev.content_hash == compute_content_hash("Apple files 8-K", "Material event.")
    assert ev.published_at == ts


def test_event_is_immutable():
    ev = make_event(
        source="s", event_type="news", ticker="X", title="t", body="b",
        url="u", published_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )
    try:
        ev.ticker = "Y"  # type: ignore[misc]
        assert False, "Event should be frozen"
    except Exception:
        pass


def test_price_label_holds_return():
    label = PriceLabel(content_hash="abc", horizon="1d", base_price=10.0,
                       future_price=11.0, return_pct=10.0)
    assert label.horizon == "1d"
    assert label.return_pct == 10.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.models'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/signal_factory/models.py`:

```python
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime

_WHITESPACE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    """Lowercase, collapse internal whitespace, strip ends."""
    return _WHITESPACE.sub(" ", text).strip().lower()


def compute_content_hash(title: str, body: str) -> str:
    """Stable sha256 over normalized title + body, robust to formatting noise."""
    payload = f"{normalize_text(title)}\n{normalize_text(body)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Event:
    source: str
    event_type: str
    ticker: str
    title: str
    body: str
    url: str
    published_at: datetime  # timezone-aware UTC
    content_hash: str


@dataclass(frozen=True)
class PriceLabel:
    content_hash: str
    horizon: str
    base_price: float
    future_price: float
    return_pct: float


def make_event(
    *,
    source: str,
    event_type: str,
    ticker: str,
    title: str,
    body: str,
    url: str,
    published_at: datetime,
) -> Event:
    """Construct an Event, computing its content hash and normalizing the ticker."""
    return Event(
        source=source,
        event_type=event_type,
        ticker=ticker.upper(),
        title=title,
        body=body,
        url=url,
        published_at=published_at,
        content_hash=compute_content_hash(title, body),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_models.py -v`
Expected: PASS (all 6 tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/models.py tests/test_models.py
git commit -m "feat: add immutable Event/PriceLabel models and content hashing"
```

---

### Task 3: SQLite storage — schema, connection, events repo

**Files:**
- Create: `src/signal_factory/storage/__init__.py`
- Create: `src/signal_factory/storage/schema.sql`
- Create: `src/signal_factory/storage/db.py`
- Create: `src/signal_factory/storage/events_repo.py`
- Test: `tests/test_storage.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_storage.py`:

```python
from datetime import datetime, timezone

from signal_factory.db_helpers import sample_event  # defined below in this test file
```

Replace the above import line — instead put the helper inline. Full file:

```python
from datetime import datetime, timezone

from signal_factory.models import make_event
from signal_factory.storage.db import connect, init_db
from signal_factory.storage.events_repo import insert_events, get_event, all_events


def _event(title="Apple files 8-K", ticker="AAPL", minute=30):
    return make_event(
        source="edgar",
        event_type="8-K",
        ticker=ticker,
        title=title,
        body="Material event.",
        url="https://example.com/1",
        published_at=datetime(2026, 6, 1, 14, minute, tzinfo=timezone.utc),
    )


def test_init_db_creates_tables():
    conn = connect(":memory:")
    init_db(conn)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in rows}
    assert {"events", "price_labels"} <= names


def test_insert_and_get_event_roundtrip():
    conn = connect(":memory:")
    init_db(conn)
    ev = _event()
    inserted = insert_events(conn, [ev])
    assert inserted == 1
    fetched = get_event(conn, ev.content_hash)
    assert fetched == ev  # full round-trip including tz-aware datetime


def test_insert_is_idempotent_on_content_hash():
    conn = connect(":memory:")
    init_db(conn)
    ev = _event()
    assert insert_events(conn, [ev]) == 1
    assert insert_events(conn, [ev]) == 0  # duplicate ignored
    assert len(all_events(conn)) == 1


def test_all_events_returns_everything():
    conn = connect(":memory:")
    init_db(conn)
    insert_events(conn, [_event(title="A"), _event(title="B")])
    assert len(all_events(conn)) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_storage.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.storage.db'`.

- [ ] **Step 3: Write minimal implementation**

Create empty `src/signal_factory/storage/__init__.py`.

Create `src/signal_factory/storage/schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS events (
    content_hash TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    title        TEXT NOT NULL,
    body         TEXT NOT NULL,
    url          TEXT NOT NULL,
    published_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_ticker ON events(ticker);
CREATE INDEX IF NOT EXISTS idx_events_published ON events(published_at);

CREATE TABLE IF NOT EXISTS price_labels (
    content_hash TEXT NOT NULL,
    horizon      TEXT NOT NULL,
    base_price   REAL NOT NULL,
    future_price REAL NOT NULL,
    return_pct   REAL NOT NULL,
    PRIMARY KEY (content_hash, horizon),
    FOREIGN KEY (content_hash) REFERENCES events(content_hash)
);
```

Create `src/signal_factory/storage/db.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with dict-like rows and FK enforcement."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables if they do not exist."""
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()
```

Create `src/signal_factory/storage/events_repo.py`:

```python
from __future__ import annotations

import sqlite3
from datetime import datetime

from signal_factory.models import Event


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        source=row["source"],
        event_type=row["event_type"],
        ticker=row["ticker"],
        title=row["title"],
        body=row["body"],
        url=row["url"],
        published_at=datetime.fromisoformat(row["published_at"]),
        content_hash=row["content_hash"],
    )


def insert_events(conn: sqlite3.Connection, events: list[Event]) -> int:
    """Insert events, ignoring duplicates by content_hash. Returns rows added."""
    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO events
            (content_hash, source, event_type, ticker, title, body, url, published_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                e.content_hash, e.source, e.event_type, e.ticker,
                e.title, e.body, e.url, e.published_at.isoformat(),
            )
            for e in events
        ],
    )
    conn.commit()
    return conn.total_changes - before


def get_event(conn: sqlite3.Connection, content_hash: str) -> Event | None:
    row = conn.execute(
        "SELECT * FROM events WHERE content_hash = ?", (content_hash,)
    ).fetchone()
    return _row_to_event(row) if row else None


def all_events(conn: sqlite3.Connection) -> list[Event]:
    rows = conn.execute("SELECT * FROM events ORDER BY published_at").fetchall()
    return [_row_to_event(r) for r in rows]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_storage.py -v`
Expected: PASS (all 4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/storage/
git add tests/test_storage.py
git commit -m "feat: add SQLite schema, connection, and events repository"
```

---

### Task 4: Deduper

**Files:**
- Create: `src/signal_factory/pipeline/__init__.py`
- Create: `src/signal_factory/pipeline/dedupe.py`
- Test: `tests/test_dedupe.py`

The same story crosses multiple wires with slightly different bodies but a near-identical
headline. Phase 0 uses a simple, deterministic rule: events are duplicates when they share a
ticker and a normalized title; keep the one with the **earliest** `published_at`. (Fuzzy
near-duplicate matching is a deliberate later enhancement.)

- [ ] **Step 1: Write the failing test**

Create `tests/test_dedupe.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import make_event
from signal_factory.pipeline.dedupe import deduplicate


def _ev(title, ticker, minute, body="b"):
    return make_event(
        source="wire", event_type="news", ticker=ticker, title=title, body=body,
        url="u", published_at=datetime(2026, 6, 1, 14, minute, tzinfo=timezone.utc),
    )


def test_keeps_earliest_of_same_story():
    late = _ev("Apple beats earnings", "AAPL", 35)
    early = _ev("  apple   beats EARNINGS ", "AAPL", 30, body="different wording")
    result = deduplicate([late, early])
    assert len(result) == 1
    assert result[0].published_at.minute == 30


def test_different_tickers_are_not_duplicates():
    a = _ev("Big news", "AAPL", 30)
    b = _ev("Big news", "MSFT", 30)
    assert len(deduplicate([a, b])) == 2


def test_different_titles_are_not_duplicates():
    a = _ev("Apple up", "AAPL", 30)
    b = _ev("Apple down", "AAPL", 31)
    assert len(deduplicate([a, b])) == 2


def test_output_is_sorted_by_published_then_hash():
    a = _ev("Title A", "AAPL", 40)
    b = _ev("Title B", "AAPL", 30)
    result = deduplicate([a, b])
    assert [e.published_at.minute for e in result] == [30, 40]


def test_empty_input():
    assert deduplicate([]) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dedupe.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.pipeline.dedupe'`.

- [ ] **Step 3: Write minimal implementation**

Create empty `src/signal_factory/pipeline/__init__.py`.

Create `src/signal_factory/pipeline/dedupe.py`:

```python
from __future__ import annotations

from signal_factory.models import Event, normalize_text


def _key(event: Event) -> tuple[str, str]:
    return (event.ticker, normalize_text(event.title))


def deduplicate(events: list[Event]) -> list[Event]:
    """Collapse same-story events (same ticker + normalized title) to the earliest.

    Output is sorted by published_at, then content_hash, for determinism.
    """
    best: dict[tuple[str, str], Event] = {}
    for event in events:
        k = _key(event)
        current = best.get(k)
        if current is None or event.published_at < current.published_at:
            best[k] = event
    return sorted(best.values(), key=lambda e: (e.published_at, e.content_hash))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dedupe.py -v`
Expected: PASS (all 5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/pipeline/__init__.py src/signal_factory/pipeline/dedupe.py tests/test_dedupe.py
git commit -m "feat: add deterministic event deduper (earliest-wins by ticker+title)"
```

---

### Task 5: DataSource interface + FixtureDataSource

**Files:**
- Create: `src/signal_factory/sources/__init__.py`
- Create: `src/signal_factory/sources/base.py`
- Create: `src/signal_factory/sources/fixture.py`
- Create: `tests/fixtures/events_sample.json`
- Test: `tests/test_sources.py`

- [ ] **Step 1: Write the failing test**

Create `tests/fixtures/events_sample.json`:

```json
[
  {
    "source": "edgar",
    "event_type": "8-K",
    "ticker": "aapl",
    "title": "Apple files 8-K on material event",
    "body": "The company disclosed a material event.",
    "url": "https://example.com/aapl-8k",
    "published_at": "2026-06-01T14:30:00+00:00"
  },
  {
    "source": "alphavantage",
    "event_type": "news",
    "ticker": "MSFT",
    "title": "Microsoft announces buyback",
    "body": "Board approves share repurchase.",
    "url": "https://example.com/msft-news",
    "published_at": "2026-06-01T15:00:00+00:00"
  }
]
```

Create `tests/test_sources.py`:

```python
from datetime import timezone
from pathlib import Path

from signal_factory.models import Event
from signal_factory.sources.fixture import FixtureDataSource

FIXTURE = Path(__file__).parent / "fixtures" / "events_sample.json"


def test_fixture_source_returns_events():
    events = FixtureDataSource(FIXTURE).fetch()
    assert len(events) == 2
    assert all(isinstance(e, Event) for e in events)


def test_fixture_source_parses_fields_and_computes_hash():
    events = FixtureDataSource(FIXTURE).fetch()
    apple = next(e for e in events if e.ticker == "AAPL")  # uppercased by make_event
    assert apple.event_type == "8-K"
    assert apple.published_at.tzinfo is not None
    assert apple.published_at.utcoffset() == timezone.utc.utcoffset(None)
    assert len(apple.content_hash) == 64
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_sources.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.sources.fixture'`.

- [ ] **Step 3: Write minimal implementation**

Create empty `src/signal_factory/sources/__init__.py`.

Create `src/signal_factory/sources/base.py`:

```python
from __future__ import annotations

from abc import ABC, abstractmethod

from signal_factory.models import Event


class DataSource(ABC):
    """A source of market events. Real adapters (EDGAR, Alpha Vantage, Finnhub)
    and test fixtures both implement this interface."""

    @abstractmethod
    def fetch(self) -> list[Event]:
        """Return the current batch of events from this source."""
        raise NotImplementedError
```

Create `src/signal_factory/sources/fixture.py`:

```python
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from signal_factory.models import Event, make_event
from signal_factory.sources.base import DataSource


class FixtureDataSource(DataSource):
    """Reads events from a local JSON file. Lets the whole pipeline be tested
    offline with no API keys or network."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def fetch(self) -> list[Event]:
        raw = json.loads(self._path.read_text(encoding="utf-8"))
        return [
            make_event(
                source=item["source"],
                event_type=item["event_type"],
                ticker=item["ticker"],
                title=item["title"],
                body=item["body"],
                url=item["url"],
                published_at=datetime.fromisoformat(item["published_at"]),
            )
            for item in raw
        ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_sources.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/sources/ tests/fixtures/events_sample.json tests/test_sources.py
git commit -m "feat: add DataSource interface and fixture-backed source"
```

---

### Task 6: Price Labeler

**Files:**
- Create: `src/signal_factory/pipeline/labeler.py`
- Test: `tests/test_labeler.py`

The labeler is the truth table: for each event it records what the price did at several
horizons. Price history comes through a `PriceProvider` interface, so this is testable with a
fake — no market-data API needed.

- [ ] **Step 1: Write the failing test**

Create `tests/test_labeler.py`:

```python
from datetime import datetime, timedelta, timezone

from signal_factory.models import make_event
from signal_factory.pipeline.labeler import PriceProvider, HORIZONS, label_event


class FakePriceProvider(PriceProvider):
    """Returns prices from a {(ticker, datetime): price} map; None if missing."""

    def __init__(self, prices):
        self._prices = prices

    def price_at(self, ticker, when):
        return self._prices.get((ticker, when))


def _event(ts):
    return make_event(
        source="edgar", event_type="8-K", ticker="AAPL", title="t", body="b",
        url="u", published_at=ts,
    )


def test_label_event_computes_return_per_horizon():
    base_time = datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc)
    prices = {("AAPL", base_time): 100.0}
    for name, delta in HORIZONS.items():
        prices[("AAPL", base_time + delta)] = 110.0
    ev = _event(base_time)

    labels = label_event(ev, FakePriceProvider(prices))

    assert len(labels) == len(HORIZONS)
    by_h = {l.horizon: l for l in labels}
    for name in HORIZONS:
        assert by_h[name].base_price == 100.0
        assert by_h[name].future_price == 110.0
        assert by_h[name].return_pct == 10.0
        assert by_h[name].content_hash == ev.content_hash


def test_label_event_skips_when_base_price_missing():
    ev = _event(datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc))
    labels = label_event(ev, FakePriceProvider({}))
    assert labels == []


def test_label_event_skips_horizon_with_missing_future_price():
    base_time = datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc)
    prices = {("AAPL", base_time): 100.0,
              ("AAPL", base_time + HORIZONS["1d"]): 105.0}
    ev = _event(base_time)
    labels = label_event(ev, FakePriceProvider(prices))
    assert {l.horizon for l in labels} == {"1d"}
    assert labels[0].return_pct == 5.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_labeler.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.pipeline.labeler'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/signal_factory/pipeline/labeler.py`:

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from signal_factory.models import Event, PriceLabel

HORIZONS: dict[str, timedelta] = {
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
    "1w": timedelta(weeks=1),
}


class PriceProvider(ABC):
    """Supplies the price of a ticker at a point in time. Real adapters wrap a
    market-data API; tests use a fake."""

    @abstractmethod
    def price_at(self, ticker: str, when: datetime) -> float | None:
        """Price at `when`, or None if unavailable."""
        raise NotImplementedError


def label_event(
    event: Event,
    provider: PriceProvider,
    horizons: dict[str, timedelta] = HORIZONS,
) -> list[PriceLabel]:
    """Label an event with forward returns at each horizon.

    Returns an empty list if the base price is unavailable. Individual horizons
    with no future price are skipped.
    """
    base_price = provider.price_at(event.ticker, event.published_at)
    if base_price is None or base_price == 0:
        return []

    labels: list[PriceLabel] = []
    for name, delta in horizons.items():
        future_price = provider.price_at(event.ticker, event.published_at + delta)
        if future_price is None:
            continue
        return_pct = (future_price - base_price) / base_price * 100.0
        labels.append(
            PriceLabel(
                content_hash=event.content_hash,
                horizon=name,
                base_price=base_price,
                future_price=future_price,
                return_pct=return_pct,
            )
        )
    return labels
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_labeler.py -v`
Expected: PASS (all 3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/pipeline/labeler.py tests/test_labeler.py
git commit -m "feat: add price labeler with PriceProvider interface and horizons"
```

---

### Task 7: Price-labels repository

**Files:**
- Create: `src/signal_factory/storage/labels_repo.py`
- Test: `tests/test_labels_repo.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_labels_repo.py`:

```python
from datetime import datetime, timezone

from signal_factory.models import make_event, PriceLabel
from signal_factory.storage.db import connect, init_db
from signal_factory.storage.events_repo import insert_events
from signal_factory.storage.labels_repo import insert_labels, labels_for


def _event():
    return make_event(
        source="edgar", event_type="8-K", ticker="AAPL", title="t", body="b",
        url="u", published_at=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )


def test_insert_and_fetch_labels():
    conn = connect(":memory:")
    init_db(conn)
    ev = _event()
    insert_events(conn, [ev])
    labels = [
        PriceLabel(ev.content_hash, "1h", 100.0, 101.0, 1.0),
        PriceLabel(ev.content_hash, "1d", 100.0, 105.0, 5.0),
    ]
    assert insert_labels(conn, labels) == 2
    fetched = labels_for(conn, ev.content_hash)
    assert {l.horizon for l in fetched} == {"1h", "1d"}


def test_insert_labels_is_idempotent_per_horizon():
    conn = connect(":memory:")
    init_db(conn)
    ev = _event()
    insert_events(conn, [ev])
    label = PriceLabel(ev.content_hash, "1d", 100.0, 105.0, 5.0)
    assert insert_labels(conn, [label]) == 1
    assert insert_labels(conn, [label]) == 0
    assert len(labels_for(conn, ev.content_hash)) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_labels_repo.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.storage.labels_repo'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/signal_factory/storage/labels_repo.py`:

```python
from __future__ import annotations

import sqlite3

from signal_factory.models import PriceLabel


def insert_labels(conn: sqlite3.Connection, labels: list[PriceLabel]) -> int:
    """Insert price labels, ignoring duplicates by (content_hash, horizon)."""
    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO price_labels
            (content_hash, horizon, base_price, future_price, return_pct)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (l.content_hash, l.horizon, l.base_price, l.future_price, l.return_pct)
            for l in labels
        ],
    )
    conn.commit()
    return conn.total_changes - before


def labels_for(conn: sqlite3.Connection, content_hash: str) -> list[PriceLabel]:
    rows = conn.execute(
        "SELECT * FROM price_labels WHERE content_hash = ? ORDER BY horizon",
        (content_hash,),
    ).fetchall()
    return [
        PriceLabel(
            content_hash=r["content_hash"],
            horizon=r["horizon"],
            base_price=r["base_price"],
            future_price=r["future_price"],
            return_pct=r["return_pct"],
        )
        for r in rows
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_labels_repo.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add src/signal_factory/storage/labels_repo.py tests/test_labels_repo.py
git commit -m "feat: add price-labels repository"
```

---

### Task 8: End-to-end pipeline runner

**Files:**
- Create: `src/signal_factory/pipeline/runner.py`
- Test: `tests/test_runner.py`

Wires the spine together: fetch from a `DataSource` → dedupe → store events → label each →
store labels. This is the function the future Watcher will call.

- [ ] **Step 1: Write the failing test**

Create `tests/test_runner.py`:

```python
from datetime import datetime, timedelta, timezone
from pathlib import Path

from signal_factory.pipeline.labeler import PriceProvider, HORIZONS
from signal_factory.pipeline.runner import run_ingest
from signal_factory.sources.fixture import FixtureDataSource
from signal_factory.storage.db import connect, init_db
from signal_factory.storage.events_repo import all_events
from signal_factory.storage.labels_repo import labels_for

FIXTURE = Path(__file__).parent / "fixtures" / "events_sample.json"


class ConstantPriceProvider(PriceProvider):
    """Always returns 100.0 for any ticker/time: a price is always present, so
    every horizon gets a label (with a 0% return). Lets us assert label counts."""

    def price_at(self, ticker, when):
        return 100.0


def test_run_ingest_stores_events_and_labels():
    conn = connect(":memory:")
    init_db(conn)
    source = FixtureDataSource(FIXTURE)
    provider = ConstantPriceProvider()

    summary = run_ingest(source, conn, provider)

    events = all_events(conn)
    assert len(events) == 2
    assert summary["events_ingested"] == 2
    # Every event gets one label per horizon (prices always present).
    total_labels = sum(len(labels_for(conn, e.content_hash)) for e in events)
    assert total_labels == 2 * len(HORIZONS)
    assert summary["labels_written"] == 2 * len(HORIZONS)


def test_run_ingest_is_idempotent():
    conn = connect(":memory:")
    init_db(conn)
    source = FixtureDataSource(FIXTURE)
    provider = ConstantPriceProvider()

    run_ingest(source, conn, provider)
    summary = run_ingest(source, conn, provider)  # second run, same data

    assert summary["events_ingested"] == 0  # nothing new
    assert len(all_events(conn)) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_runner.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'signal_factory.pipeline.runner'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/signal_factory/pipeline/runner.py`:

```python
from __future__ import annotations

import sqlite3

from signal_factory.pipeline.dedupe import deduplicate
from signal_factory.pipeline.labeler import PriceProvider, label_event
from signal_factory.sources.base import DataSource
from signal_factory.storage.events_repo import insert_events
from signal_factory.storage.labels_repo import insert_labels


def run_ingest(
    source: DataSource,
    conn: sqlite3.Connection,
    provider: PriceProvider,
) -> dict[str, int]:
    """Fetch -> dedupe -> store events -> label -> store labels.

    Returns a summary dict: events_ingested (newly stored), labels_written.
    """
    fetched = source.fetch()
    deduped = deduplicate(fetched)
    events_ingested = insert_events(conn, deduped)

    labels_written = 0
    for event in deduped:
        labels = label_event(event, provider)
        labels_written += insert_labels(conn, labels)

    return {
        "events_fetched": len(fetched),
        "events_deduped": len(deduped),
        "events_ingested": events_ingested,
        "labels_written": labels_written,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_runner.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -v`
Expected: PASS — all tests across every file green.

- [ ] **Step 6: Commit**

```bash
git add src/signal_factory/pipeline/runner.py tests/test_runner.py
git commit -m "feat: add end-to-end ingest pipeline runner"
```

---

## Definition of Done (Phase 0)

- `python -m pytest -v` is fully green.
- A `DataSource` can be ingested end-to-end into SQLite with events deduped, timestamped, and
  price-labeled, entirely offline via fixtures and a fake price provider.
- All three boundary interfaces exist and are honored: `DataSource`, `PriceProvider`, and the
  storage repositories.

## What Phase 0 deliberately excludes (future plans)

- Real data adapters (EDGAR / Alpha Vantage / Finnhub `DataSource` implementations) and a real
  `PriceProvider` — these need network and API keys; they slot in behind the interfaces built here.
- The Watcher daemon, the discovery feed, the Intelligence agents, the Risk Manager, and the
  Paper/Live Executors.
- Fuzzy near-duplicate detection (Phase 0 dedupe is exact-match on ticker + normalized title).

---

## Self-Review

**Spec coverage (spec §4 deterministic spine):** Ingestor → `runner.run_ingest` + `DataSource`
(Tasks 5, 8). Deduper + Timestamper → Task 4 + tz-aware datetimes carried through Tasks 2–3.
Price Labeler (truth table) → Task 6. Storage / data model (`events`, `price_labels`) → Tasks
2, 3, 7. Backtester, Risk Manager, Executor, Intelligence agents, Watcher → explicitly deferred
to later plans (documented above), consistent with the spec's phased build order (§9).

**Placeholder scan:** No TBD/TODO/"handle edge cases" — every code step shows complete code and
every run step shows the exact command and expected result.

**Type consistency:** `Event`, `PriceLabel`, `make_event`, `normalize_text`,
`compute_content_hash` (Task 2) are used with identical signatures in Tasks 3–8. `DataSource.fetch`
(Task 5), `PriceProvider.price_at` + `HORIZONS` + `label_event` (Task 6), `insert_events`/`get_event`/
`all_events` (Task 3), `insert_labels`/`labels_for` (Task 7), and `run_ingest` (Task 8) match across
their callers. SQLite columns in `schema.sql` match every repo query.
