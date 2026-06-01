from __future__ import annotations

from abc import ABC, abstractmethod

from signal_factory.models import Event


class DataSource(ABC):
    """A source of market events. Real adapters (EDGAR, Alpha Vantage, Finnhub)
    and test fixtures both implement this interface."""

    @abstractmethod
    def fetch(self) -> list[Event]:
        """Return the current batch of events from this source."""
        raise NotImplementedError
