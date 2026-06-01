from datetime import datetime, timedelta, timezone

from signal_factory.models import make_event
from signal_factory.pipeline.labeler import PriceProvider, HORIZONS, label_event


class FakePriceProvider(PriceProvider):
    """Returns prices from a {(ticker, datetime): price} map; None if missing."""

    def __init__(self, prices):
        self._prices = prices

    def price_at(self, ticker, when):
        return self._prices.get((ticker, when))


def _event(ts):
    return make_event(
        source="edgar", event_type="8-K", ticker="AAPL", title="t", body="b",
        url="u", published_at=ts,
    )


def test_label_event_computes_return_per_horizon():
    base_time = datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc)
    prices = {("AAPL", base_time): 100.0}
    for name, delta in HORIZONS.items():
        prices[("AAPL", base_time + delta)] = 110.0
    ev = _event(base_time)

    labels = label_event(ev, FakePriceProvider(prices))

    assert len(labels) == len(HORIZONS)
    by_h = {l.horizon: l for l in labels}
    for name in HORIZONS:
        assert by_h[name].base_price == 100.0
        assert by_h[name].future_price == 110.0
        assert by_h[name].return_pct == 10.0
        assert by_h[name].content_hash == ev.content_hash


def test_label_event_skips_when_base_price_missing():
    ev = _event(datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc))
    labels = label_event(ev, FakePriceProvider({}))
    assert labels == []


def test_label_event_skips_horizon_with_missing_future_price():
    base_time = datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc)
    prices = {("AAPL", base_time): 100.0,
              ("AAPL", base_time + HORIZONS["1d"]): 105.0}
    ev = _event(base_time)
    labels = label_event(ev, FakePriceProvider(prices))
    assert {l.horizon for l in labels} == {"1d"}
    assert labels[0].return_pct == 5.0
