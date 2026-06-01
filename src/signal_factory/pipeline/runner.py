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
