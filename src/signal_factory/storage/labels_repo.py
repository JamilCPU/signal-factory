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
