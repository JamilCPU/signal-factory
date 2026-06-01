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
