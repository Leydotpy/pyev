"""Idempotency store contracts and an async in-memory implementation."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, runtime_checkable


class IdempotencyStatus(StrEnum):
    """Processing state of an idempotency key."""

    PROCESSING = "processing"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    """Stored state for one idempotent operation."""

    key: str
    status: IdempotencyStatus
    acquired_at: float
    expires_at: float
    completed_at: float | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("key must not be empty")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def expired(self, now: float) -> bool:
        """Return whether this record is past its expiry."""

        return self.expires_at <= now


@runtime_checkable
class IdempotencyStore(Protocol):
    """Persistence contract for inbox/idempotency records."""

    async def claim(
        self,
        key: str,
        *,
        ttl: float | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> bool:
        """Atomically claim a key if no unexpired record exists."""

    async def complete(self, key: str, *, ttl: float | None = None) -> None:
        """Mark a claimed key complete."""

    async def release(self, key: str) -> bool:
        """Release an in-progress claim after failed processing."""

    async def get(self, key: str) -> IdempotencyRecord | None:
        """Return an unexpired record for ``key``."""

    async def purge_expired(self) -> int:
        """Delete and return the count of expired records."""


class MemoryIdempotencyStore:
    """Bounded, process-local idempotency store for tests and local runtime."""

    def __init__(
        self,
        *,
        default_ttl: float = 86_400.0,
        processing_ttl: float = 300.0,
        max_entries: int = 100_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if default_ttl <= 0 or processing_ttl <= 0:
            raise ValueError("TTLs must be positive")
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        self.default_ttl = float(default_ttl)
        self.processing_ttl = float(processing_ttl)
        self.max_entries = max_entries
        self._clock = clock
        self._records: dict[str, IdempotencyRecord] = {}
        self._lock = asyncio.Lock()

    async def claim(
        self,
        key: str,
        *,
        ttl: float | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> bool:
        if not key:
            raise ValueError("key must not be empty")
        claim_ttl = self.processing_ttl if ttl is None else ttl
        if claim_ttl <= 0:
            raise ValueError("ttl must be positive")
        async with self._lock:
            now = float(self._clock())
            existing = self._records.get(key)
            if existing is not None and not existing.expired(now):
                return False
            if existing is not None:
                del self._records[key]
            await self._make_capacity_locked(now)
            self._records[key] = IdempotencyRecord(
                key=key,
                status=IdempotencyStatus.PROCESSING,
                acquired_at=now,
                expires_at=now + claim_ttl,
                metadata=metadata or {},
            )
            return True

    async def complete(self, key: str, *, ttl: float | None = None) -> None:
        completion_ttl = self.default_ttl if ttl is None else ttl
        if completion_ttl <= 0:
            raise ValueError("ttl must be positive")
        async with self._lock:
            now = float(self._clock())
            record = self._records.get(key)
            if record is None or record.expired(now):
                raise KeyError(f"idempotency key {key!r} is not claimed")
            self._records[key] = replace(
                record,
                status=IdempotencyStatus.COMPLETED,
                completed_at=now,
                expires_at=now + completion_ttl,
            )

    async def release(self, key: str) -> bool:
        async with self._lock:
            record = self._records.get(key)
            if record is None or record.status is IdempotencyStatus.COMPLETED:
                return False
            del self._records[key]
            return True

    async def get(self, key: str) -> IdempotencyRecord | None:
        async with self._lock:
            now = float(self._clock())
            record = self._records.get(key)
            if record is not None and record.expired(now):
                del self._records[key]
                return None
            return record

    async def contains(self, key: str, *, completed_only: bool = False) -> bool:
        """Return whether an active record exists for a key."""

        record = await self.get(key)
        return record is not None and (
            not completed_only or record.status is IdempotencyStatus.COMPLETED
        )

    async def purge_expired(self) -> int:
        async with self._lock:
            now = float(self._clock())
            expired = [key for key, record in self._records.items() if record.expired(now)]
            for key in expired:
                del self._records[key]
            return len(expired)

    async def _make_capacity_locked(self, now: float) -> None:
        expired = [key for key, record in self._records.items() if record.expired(now)]
        for key in expired:
            del self._records[key]
        if len(self._records) >= self.max_entries:
            raise OverflowError("idempotency store capacity has been reached")


__all__ = [
    "IdempotencyRecord",
    "IdempotencyStatus",
    "IdempotencyStore",
    "MemoryIdempotencyStore",
]
