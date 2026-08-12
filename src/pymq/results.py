"""Result value objects returned by broker operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class PublishResult:
    """The portable result of a successful engine publish."""

    message_id: str
    route: str
    destination: str
    engine: str
    published_at: datetime
    accepted: bool = True
    transport_id: str | None = None


@dataclass(frozen=True, slots=True)
class BatchItemError:
    """One failed item in a batch publish."""

    index: int
    error: Exception


@dataclass(frozen=True, slots=True)
class BatchPublishResult:
    """Ordered successes and indexed failures from a batch publish."""

    results: tuple[PublishResult, ...] = ()
    errors: tuple[BatchItemError, ...] = ()

    @property
    def successful(self) -> int:
        """Return the number of successfully published items."""

        return len(self.results)

    @property
    def failed(self) -> int:
        """Return the number of failed items."""

        return len(self.errors)

    @property
    def ok(self) -> bool:
        """Return whether every item was published."""

        return not self.errors
