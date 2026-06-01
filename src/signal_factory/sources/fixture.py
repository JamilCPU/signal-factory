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
