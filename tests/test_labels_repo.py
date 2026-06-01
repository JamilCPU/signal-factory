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
