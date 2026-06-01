from datetime import timezone
from pathlib import Path

from signal_factory.models import Event
from signal_factory.sources.fixture import FixtureDataSource

FIXTURE = Path(__file__).parent / "fixtures" / "events_sample.json"


def test_fixture_source_returns_events():
    events = FixtureDataSource(FIXTURE).fetch()
    assert len(events) == 2
    assert all(isinstance(e, Event) for e in events)


def test_fixture_source_parses_fields_and_computes_hash():
    events = FixtureDataSource(FIXTURE).fetch()
    apple = next(e for e in events if e.ticker == "AAPL")  # uppercased by make_event
    assert apple.event_type == "8-K"
    assert apple.published_at.tzinfo is not None
    assert apple.published_at.utcoffset() == timezone.utc.utcoffset(None)
    assert len(apple.content_hash) == 64
