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
