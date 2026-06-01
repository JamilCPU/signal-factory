from datetime import datetime, timezone

from signal_factory.models import (
    Event,
    PriceLabel,
    normalize_text,
    compute_content_hash,
    make_event,
)


def test_normalize_text_lowercases_and_collapses_whitespace():
    assert normalize_text("  Apple   Beats\n Earnings ") == "apple beats earnings"


def test_content_hash_is_stable_and_ignores_formatting():
    a = compute_content_hash("Apple Beats", "Body text")
    b = compute_content_hash("  apple   beats ", "body   text")
    assert a == b
    assert len(a) == 64  # sha256 hex


def test_content_hash_differs_on_different_content():
    assert compute_content_hash("Apple up", "x") != compute_content_hash("Apple down", "x")


def test_make_event_computes_hash_and_uppercases_ticker():
    ts = datetime(2026, 6, 1, 14, 30, tzinfo=timezone.utc)
    ev = make_event(
        source="edgar",
        event_type="8-K",
        ticker="aapl",
        title="Apple files 8-K",
        body="Material event.",
        url="https://example.com/1",
        published_at=ts,
    )
    assert isinstance(ev, Event)
    assert ev.ticker == "AAPL"
    assert ev.content_hash == compute_content_hash("Apple files 8-K", "Material event.")
    assert ev.published_at == ts


def test_event_is_immutable():
    ev = make_event(
        source="s", event_type="news", ticker="X", title="t", body="b",
        url="u", published_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )
    try:
        ev.ticker = "Y"  # type: ignore[misc]
        assert False, "Event should be frozen"
    except Exception:
        pass


def test_price_label_holds_return():
    label = PriceLabel(content_hash="abc", horizon="1d", base_price=10.0,
                       future_price=11.0, return_pct=10.0)
    assert label.horizon == "1d"
    assert label.return_pct == 10.0
