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
