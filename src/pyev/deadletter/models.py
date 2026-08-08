"""Dead-letter records, filters, and retention policy values."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from fnmatch import fnmatchcase
from types import MappingProxyType
from typing import TYPE_CHECKING
from uuid import uuid4

from pyev.observability.redaction import redact_mapping, redact_text

if TYPE_CHECKING:
    from pyev.envelope import Envelope


class DeadLetterStatus(StrEnum):
    """Administrative state of a dead-letter record."""

    ACTIVE = "active"
    QUARANTINED = "quarantined"
    ARCHIVED = "archived"
    REPLAYED = "replayed"


@dataclass(frozen=True, slots=True)
class RetryHistoryEntry:
    """Sanitized history for one previous processing attempt."""

    attempt: int
    error_type: str
    error_message: str
    timestamp: datetime
    next_attempt_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.attempt < 1:
            raise ValueError("attempt must be at least 1")
        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        if self.next_attempt_at is not None and self.next_attempt_at.tzinfo is None:
            raise ValueError("next_attempt_at must be timezone-aware")
        object.__setattr__(self, "error_message", redact_text(self.error_message))


@dataclass(frozen=True, slots=True)
class DeadLetterContext:
    """Failure and delivery metadata accepted by :class:`DeadLetterManager`."""

    route: str | None = None
    destination: str | None = None
    headers: Mapping[str, object] = field(default_factory=dict)
    metadata: Mapping[str, object] = field(default_factory=dict)
    retry_history: tuple[RetryHistoryEntry, ...] = ()
    first_failed_at: datetime | None = None
    consumer: str | None = None
    subscription: str | None = None
    engine: str | None = None
    failure_classification: str = "terminal"
    decoded_payload: object | None = None
    original_envelope_bytes: bytes | None = None

    def __post_init__(self) -> None:
        if self.first_failed_at is not None and self.first_failed_at.tzinfo is None:
            raise ValueError("first_failed_at must be timezone-aware")
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class DeadLetterPolicy:
    """Security, replay, and retention rules for dead-letter persistence."""

    include_decoded_payload: bool = False
    include_traceback: bool = True
    traceback_limit: int = 8_192
    sensitive_headers: frozenset[str] = frozenset()
    max_replay_attempts: int = 3
    max_replay_batch: int = 1_000
    replay_rate_limit: float | None = None
    quarantine_after_replay_failures: int = 2
    archive_after: timedelta | None = timedelta(days=30)
    retention: timedelta | None = timedelta(days=90)

    def __post_init__(self) -> None:
        if self.traceback_limit < 0:
            raise ValueError("traceback_limit must be non-negative")
        if self.max_replay_attempts < 1 or self.max_replay_batch < 1:
            raise ValueError("replay attempt and batch limits must be at least 1")
        if self.replay_rate_limit is not None and self.replay_rate_limit <= 0:
            raise ValueError("replay_rate_limit must be positive")
        if self.quarantine_after_replay_failures < 1:
            raise ValueError("quarantine_after_replay_failures must be at least 1")
        for name in ("archive_after", "retention"):
            value = getattr(self, name)
            if value is not None and value <= timedelta(0):
                raise ValueError(f"{name} must be positive")
        if self.archive_after and self.retention and self.archive_after > self.retention:
            raise ValueError("archive_after cannot exceed retention")


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    """Immutable transport-independent dead-letter record."""

    envelope: Envelope | object | None
    envelope_bytes: bytes | None
    error_type: str
    error_message: str
    id: str = field(default_factory=lambda: str(uuid4()))
    route: str | None = None
    destination: str | None = None
    headers: Mapping[str, object] = field(default_factory=dict)
    metadata: Mapping[str, object] = field(default_factory=dict)
    traceback: str | None = None
    retry_history: tuple[RetryHistoryEntry, ...] = ()
    first_failed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    dead_lettered_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    consumer: str | None = None
    subscription: str | None = None
    engine: str | None = None
    failure_classification: str = "terminal"
    event_type: str | None = None
    schema_version: int | None = None
    serializer: str | None = None
    decoded_payload: object | None = None
    status: DeadLetterStatus = DeadLetterStatus.ACTIVE
    quarantine_reason: str | None = None
    archive_reason: str | None = None
    replay_count: int = 0
    replay_failures: int = 0
    last_replay_at: datetime | None = None
    last_replay_error: str | None = None

    def __post_init__(self) -> None:
        if not self.id or not self.error_type:
            raise ValueError("id and error_type must not be empty")
        if self.replay_count < 0 or self.replay_failures < 0:
            raise ValueError("replay counters must be non-negative")
        timestamps = (
            self.first_failed_at,
            self.dead_lettered_at,
            self.updated_at,
            self.last_replay_at,
        )
        if any(value is not None and value.tzinfo is None for value in timestamps):
            raise ValueError("dead-letter timestamps must be timezone-aware")
        object.__setattr__(self, "headers", MappingProxyType(redact_mapping(self.headers)))
        object.__setattr__(self, "metadata", MappingProxyType(redact_mapping(self.metadata)))
        object.__setattr__(self, "error_message", redact_text(self.error_message))
        if self.traceback is not None:
            object.__setattr__(self, "traceback", redact_text(self.traceback))
        for field_name in ("quarantine_reason", "archive_reason", "last_replay_error"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, redact_text(value))


@dataclass(frozen=True, slots=True)
class DeadLetterFilter:
    """Portable query predicates for a dead-letter store."""

    ids: frozenset[str] | None = None
    statuses: frozenset[DeadLetterStatus] | None = None
    route_pattern: str | None = None
    destination: str | None = None
    engine: str | None = None
    failure_classification: str | None = None
    event_type: str | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    quarantined: bool | None = None
    limit: int | None = None

    def __post_init__(self) -> None:
        if self.limit is not None and self.limit < 1:
            raise ValueError("limit must be at least 1")
        if self.created_after is not None and self.created_after.tzinfo is None:
            raise ValueError("created_after must be timezone-aware")
        if self.created_before is not None and self.created_before.tzinfo is None:
            raise ValueError("created_before must be timezone-aware")

    def matches(self, record: DeadLetterRecord) -> bool:
        """Return whether a record satisfies every configured predicate."""

        if self.ids is not None and record.id not in self.ids:
            return False
        if self.statuses is not None and record.status not in self.statuses:
            return False
        if self.route_pattern is not None and not fnmatchcase(
            record.route or "", self.route_pattern
        ):
            return False
        if self.destination is not None and record.destination != self.destination:
            return False
        if self.engine is not None and record.engine != self.engine:
            return False
        if (
            self.failure_classification is not None
            and record.failure_classification != self.failure_classification
        ):
            return False
        if self.event_type is not None and record.event_type != self.event_type:
            return False
        if self.created_after is not None and record.dead_lettered_at < self.created_after:
            return False
        if self.created_before is not None and record.dead_lettered_at >= self.created_before:
            return False
        if self.quarantined is not None:
            is_quarantined = record.status is DeadLetterStatus.QUARANTINED
            if is_quarantined is not self.quarantined:
                return False
        return True


__all__ = [
    "DeadLetterContext",
    "DeadLetterFilter",
    "DeadLetterPolicy",
    "DeadLetterRecord",
    "DeadLetterStatus",
    "RetryHistoryEntry",
]
