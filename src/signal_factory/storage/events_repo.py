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
