from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from signal_factory.models import Event, PriceLabel

HORIZONS: dict[str, timedelta] = {
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
    "1w": timedelta(weeks=1),
}


class PriceProvider(ABC):
    """Supplies the price of a ticker at a point in time. Real adapters wrap a
    market-data API; tests use a fake."""

    @abstractmethod
    def price_at(self, ticker: str, when: datetime) -> float | None:
        """Price at `when`, or None if unavailable."""
        raise NotImplementedError


def label_event(
    event: Event,
    provider: PriceProvider,
    horizons: dict[str, timedelta] = HORIZONS,
) -> list[PriceLabel]:
    """Label an event with forward returns at each horizon.

    Returns an empty list if the base price is unavailable. Individual horizons
    with no future price are skipped.
    """
    base_price = provider.price_at(event.ticker, event.published_at)
    if base_price is None or base_price == 0:
        return []

    labels: list[PriceLabel] = []
    for name, delta in horizons.items():
        future_price = provider.price_at(event.ticker, event.published_at + delta)
        if future_price is None:
            continue
        return_pct = (future_price - base_price) / base_price * 100.0
        labels.append(
            PriceLabel(
                content_hash=event.content_hash,
                horizon=name,
                base_price=base_price,
                future_price=future_price,
                return_pct=return_pct,
            )
        )
    return labels
