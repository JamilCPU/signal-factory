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
