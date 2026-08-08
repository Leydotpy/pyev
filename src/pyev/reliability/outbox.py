"""Transactional outbox contracts and process-local reference storage."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, runtime_checkable
from uuid import uuid4

from pyev.observability.redaction import redact_text

from .backoff import BackoffStrategy, ExponentialFullJitterBackoff, calculate_backoff


class OutboxStatus(StrEnum):
    """Persistence state of an outbox message."""

    PENDING = "pending"
    LEASED = "leased"
    PUBLISHED = "published"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """A message waiting to be dispatched after application state commits."""

    envelope: object
    destination: str
    id: str = field(default_factory=lambda: str(uuid4()))
    status: OutboxStatus = OutboxStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    available_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    attempts: int = 0
    last_error: str | None = None
    lease_id: str | None = None
    leased_until: datetime | None = None
    published_at: datetime | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("id must not be empty")
        if not self.destination:
            raise ValueError("destination must not be empty")
        if self.attempts < 0:
            raise ValueError("attempts must be non-negative")
        for value in (self.created_at, self.available_at, self.leased_until, self.published_at):
            if value is not None and value.tzinfo is None:
                raise ValueError("outbox timestamps must be timezone-aware")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@runtime_checkable
class OutboxStore(Protocol):
    """Persistence contract required by :class:`OutboxDispatcher`."""

    async def add(self, message: OutboxMessage) -> None:
        """Persist a message atomically in the application's transaction."""

    async def get(self, message_id: str) -> OutboxMessage | None:
        """Return a stored message."""

    async def lease(
        self,
        *,
        limit: int,
        lease_duration: float,
        now: datetime | None = None,
    ) -> Sequence[OutboxMessage]:
        """Atomically lease available messages for one dispatcher."""

    async def mark_published(self, message_id: str, lease_id: str) -> None:
        """Mark a leased message successfully published."""

    async def mark_failed(
        self,
        message_id: str,
        lease_id: str,
        *,
        error: str,
        retry_at: datetime | None,
        terminal: bool = False,
    ) -> None:
        """Release or terminally fail a leased message."""


class MemoryOutboxStore:
    """Bounded in-memory outbox with atomic leasing semantics."""

    def __init__(self, *, max_entries: int = 100_000) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        self.max_entries = max_entries
        self._messages: dict[str, OutboxMessage] = {}
        self._lock = asyncio.Lock()

    async def add(self, message: OutboxMessage) -> None:
        async with self._lock:
            if message.id in self._messages:
                raise ValueError(f"outbox message {message.id!r} already exists")
            if len(self._messages) >= self.max_entries:
                raise OverflowError("outbox store capacity has been reached")
            self._messages[message.id] = message

    async def get(self, message_id: str) -> OutboxMessage | None:
        async with self._lock:
            return self._messages.get(message_id)

    async def lease(
        self,
        *,
        limit: int,
        lease_duration: float,
        now: datetime | None = None,
    ) -> Sequence[OutboxMessage]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if lease_duration <= 0:
            raise ValueError("lease_duration must be positive")
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        leased: list[OutboxMessage] = []
        async with self._lock:
            # Expired leases are recoverable after a crashed dispatcher.
            for message_id, message in tuple(self._messages.items()):
                if (
                    message.status is OutboxStatus.LEASED
                    and message.leased_until is not None
                    and message.leased_until <= current
                ):
                    self._messages[message_id] = replace(
                        message,
                        status=OutboxStatus.FAILED,
                        lease_id=None,
                        leased_until=None,
                    )
            candidates = sorted(
                (
                    message
                    for message in self._messages.values()
                    if message.status in (OutboxStatus.PENDING, OutboxStatus.FAILED)
                    and message.available_at <= current
                ),
                key=lambda item: (item.available_at, item.created_at, item.id),
            )
            for message in candidates[:limit]:
                lease_id = str(uuid4())
                updated = replace(
                    message,
                    status=OutboxStatus.LEASED,
                    lease_id=lease_id,
                    leased_until=current + timedelta(seconds=lease_duration),
                    attempts=message.attempts + 1,
                )
                self._messages[message.id] = updated
                leased.append(updated)
        return tuple(leased)

    async def mark_published(self, message_id: str, lease_id: str) -> None:
        async with self._lock:
            message = self._require_lease_locked(message_id, lease_id)
            self._messages[message_id] = replace(
                message,
                status=OutboxStatus.PUBLISHED,
                published_at=datetime.now(UTC),
                lease_id=None,
                leased_until=None,
                last_error=None,
            )

    async def mark_failed(
        self,
        message_id: str,
        lease_id: str,
        *,
        error: str,
        retry_at: datetime | None,
        terminal: bool = False,
    ) -> None:
        if retry_at is not None and retry_at.tzinfo is None:
            raise ValueError("retry_at must be timezone-aware")
        async with self._lock:
            message = self._require_lease_locked(message_id, lease_id)
            self._messages[message_id] = replace(
                message,
                status=(OutboxStatus.DEAD_LETTERED if terminal else OutboxStatus.FAILED),
                available_at=retry_at or message.available_at,
                last_error=error,
                lease_id=None,
                leased_until=None,
            )

    async def list(
        self,
        *,
        status: OutboxStatus | None = None,
    ) -> tuple[OutboxMessage, ...]:
        """Inspect messages in deterministic creation order."""

        async with self._lock:
            values = (
                message
                for message in self._messages.values()
                if status is None or message.status is status
            )
            return tuple(sorted(values, key=lambda item: (item.created_at, item.id)))

    async def purge_published(self, *, before: datetime) -> int:
        """Remove published records older than ``before``."""

        if before.tzinfo is None:
            raise ValueError("before must be timezone-aware")
        async with self._lock:
            ids = [
                message.id
                for message in self._messages.values()
                if message.status is OutboxStatus.PUBLISHED
                and message.published_at is not None
                and message.published_at < before
            ]
            for message_id in ids:
                del self._messages[message_id]
            return len(ids)

    def _require_lease_locked(self, message_id: str, lease_id: str) -> OutboxMessage:
        message = self._messages.get(message_id)
        if message is None:
            raise KeyError(f"unknown outbox message {message_id!r}")
        if message.status is not OutboxStatus.LEASED or message.lease_id != lease_id:
            raise ValueError("outbox lease no longer belongs to this dispatcher")
        return message


PublishOutboxMessage = Callable[[OutboxMessage], Awaitable[None]]


class OutboxDispatcher:
    """Lease and publish outbox records with bounded batches and retry delay."""

    def __init__(
        self,
        store: OutboxStore,
        publish: PublishOutboxMessage,
        *,
        batch_size: int = 100,
        lease_duration: float = 30.0,
        max_attempts: int = 10,
        backoff: BackoffStrategy | None = None,
        idle_delay: float = 1.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if batch_size < 1 or max_attempts < 1:
            raise ValueError("batch_size and max_attempts must be at least 1")
        if lease_duration <= 0 or idle_delay < 0:
            raise ValueError("lease_duration must be positive and idle_delay non-negative")
        self.store = store
        self._publish = publish
        self.batch_size = batch_size
        self.lease_duration = lease_duration
        self.max_attempts = max_attempts
        self.backoff = backoff or ExponentialFullJitterBackoff()
        self.idle_delay = idle_delay
        self._sleep = sleep

    async def dispatch_once(self) -> int:
        """Dispatch one bounded batch and return the number of leased records."""

        messages = await self.store.lease(
            limit=self.batch_size,
            lease_duration=self.lease_duration,
        )
        for message in messages:
            assert message.lease_id is not None
            try:
                await self._publish(message)
            except asyncio.CancelledError:
                await self.store.mark_failed(
                    message.id,
                    message.lease_id,
                    error="dispatcher cancelled",
                    retry_at=datetime.now(UTC),
                )
                raise
            except Exception as error:
                terminal = message.attempts >= self.max_attempts
                delay = calculate_backoff(self.backoff, max(1, message.attempts))
                await self.store.mark_failed(
                    message.id,
                    message.lease_id,
                    error=redact_text(f"{type(error).__name__}: {error}"),
                    retry_at=None if terminal else datetime.now(UTC) + timedelta(seconds=delay),
                    terminal=terminal,
                )
            else:
                await self.store.mark_published(message.id, message.lease_id)
        return len(messages)

    async def run(self, stop: asyncio.Event) -> None:
        """Dispatch until ``stop`` is set; intended for a task supervisor."""

        while not stop.is_set():
            count = await self.dispatch_once()
            if count == 0:
                await self._sleep(self.idle_delay)


__all__ = [
    "MemoryOutboxStore",
    "OutboxDispatcher",
    "OutboxMessage",
    "OutboxStatus",
    "OutboxStore",
    "PublishOutboxMessage",
]
