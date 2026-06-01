from datetime import datetime, timezone

from signal_factory.models import make_event
from signal_factory.pipeline.dedupe import deduplicate


def _ev(title, ticker, minute, body="b"):
    return make_event(
        source="wire", event_type="news", ticker=ticker, title=title, body=body,
        url="u", published_at=datetime(2026, 6, 1, 14, minute, tzinfo=timezone.utc),
    )


def test_keeps_earliest_of_same_story():
    late = _ev("Apple beats earnings", "AAPL", 35)
    early = _ev("  apple   beats EARNINGS ", "AAPL", 30, body="different wording")
    result = deduplicate([late, early])
    assert len(result) == 1
    assert result[0].published_at.minute == 30


def test_different_tickers_are_not_duplicates():
    a = _ev("Big news", "AAPL", 30)
    b = _ev("Big news", "MSFT", 30)
    assert len(deduplicate([a, b])) == 2


def test_different_titles_are_not_duplicates():
    a = _ev("Apple up", "AAPL", 30)
    b = _ev("Apple down", "AAPL", 31)
    assert len(deduplicate([a, b])) == 2


def test_output_is_sorted_by_published_then_hash():
    a = _ev("Title A", "AAPL", 40)
    b = _ev("Title B", "AAPL", 30)
    result = deduplicate([a, b])
    assert [e.published_at.minute for e in result] == [30, 40]


def test_empty_input():
    assert deduplicate([]) == []
