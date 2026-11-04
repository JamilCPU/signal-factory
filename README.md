# Signal Factory

A research and signal-generation engine for studying whether business events (SEC filings and official news, not social media) carry information about subsequent price moves. It ingests events, removes duplicates while keeping the earliest timestamp, and labels each event with what the price did afterwards, producing a "truth table" that can be used to measure whether a signal works.

Signal Factory is research tooling. It does not place trades, and it contains no broker integration. Any future execution step is gated behind explicit human approval by design.

## What exists today (Phase 0, the data spine)

- **Event model**: immutable `Event` records with a stable content hash over normalised title and body.
- **Sources**: a `DataSource` interface plus a fixture-backed source that reads events from JSON, so the whole pipeline runs offline with no API keys. Real source adapters are not written yet.
- **Dedupe**: deterministic, earliest-wins by ticker and title, so a signal is stamped with the first time it was seen.
- **Labelling**: for each event, forward returns at 5 minutes, 1 hour, 1 day and 1 week, computed through a `PriceProvider` interface (tests use a fake; no market-data adapter is written yet).
- **Storage**: SQLite tables for events and price labels, with repositories.
- **Runner**: `run_ingest` chains fetch, dedupe, store, label and store, and returns a summary of counts.

## Stack

- Python 3.11+ (standard library only at runtime: `sqlite3`, `dataclasses`)
- SQLite for storage
- pytest

## Running

```
pip install -e ".[dev]"
python -m pytest
```

Minimal use from Python:

```python
from signal_factory.pipeline.labeler import PriceProvider
from signal_factory.pipeline.runner import run_ingest
from signal_factory.sources.fixture import FixtureDataSource
from signal_factory.storage.db import connect, init_db

class FlatPrices(PriceProvider):
    def price_at(self, ticker, when):
        return 100.0

conn = connect(":memory:")
init_db(conn)
print(run_ingest(FixtureDataSource("tests/fixtures/events_sample.json"), conn, FlatPrices()))
```

See `tests/test_runner.py` for more worked examples.

## Project layout

| Path | What |
|------|------|
| `src/signal_factory/models.py` | `Event`, `PriceLabel`, content hashing |
| `src/signal_factory/sources/` | `DataSource` interface and fixture source |
| `src/signal_factory/pipeline/` | deduper, labeller, ingest runner |
| `src/signal_factory/storage/` | SQLite schema, connection, repositories |
| `docs/superpowers/` | design specs and implementation plans |

## Status

Work in progress. Phase 0 is built and tested. A historical replay simulator (a no-lookahead replay of past events against the labels, with a virtual portfolio and accuracy report) is designed in `docs/superpowers/specs/2026-06-02-replay-simulator-design.md` and its first implementation plan is written, but the replay engine is not implemented yet. No live data feeds, no discovery feed and no execution are implemented. Nothing here is investment advice, and no performance results are claimed.
