"""Dead-letter store protocol and bounded in-memory implementation."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from .models import DeadLetterFilter, DeadLetterRecord, DeadLetterStatus


@runtime_checkable
class DeadLetterStore(Protocol):
    """Persistence contract for dead-letter administration."""

    async def put(self, record: DeadLetterRecord) -> None:
        """Persist a new record."""

    async def get(self, record_id: str) -> DeadLetterRecord | None:
        """Return one record."""

    async def query(self, filters: DeadLetterFilter | None = None) -> Sequence[DeadLetterRecord]:
        """Return records matching a portable filter."""

    async def update(self, record: DeadLetterRecord) -> None:
        """Replace an existing record."""

    async def delete(self, record_id: str) -> bool:
        """Delete one record."""

    async def count(self, filters: DeadLetterFilter | None = None) -> int:
        """Count matching records."""


class MemoryDeadLetterStore:
    """Bounded, concurrency-safe process-local dead-letter storage."""

    def __init__(self, *, max_entries: int = 100_000) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        self.max_entries = max_entries
        self._records: dict[str, DeadLetterRecord] = {}
        self._lock = asyncio.Lock()

    async def put(self, record: DeadLetterRecord) -> None:
        async with self._lock:
            if record.id in self._records:
                raise ValueError(f"dead-letter record {record.id!r} already exists")
            if len(self._records) >= self.max_entries:
                raise OverflowError("dead-letter store capacity has been reached")
            self._records[record.id] = record

    async def get(self, record_id: str) -> DeadLetterRecord | None:
        async with self._lock:
            return self._records.get(record_id)

    async def query(self, filters: DeadLetterFilter | None = None) -> Sequence[DeadLetterRecord]:
        selected_filter = filters or DeadLetterFilter()
        async with self._lock:
            records = sorted(
                (record for record in self._records.values() if selected_filter.matches(record)),
                key=lambda item: (item.dead_lettered_at, item.id),
            )
            if selected_filter.limit is not None:
                records = records[: selected_filter.limit]
            return tuple(records)

    inspect = query

    async def update(self, record: DeadLetterRecord) -> None:
        async with self._lock:
            if record.id not in self._records:
                raise KeyError(f"unknown dead-letter record {record.id!r}")
            self._records[record.id] = record

    async def delete(self, record_id: str) -> bool:
        async with self._lock:
            return self._records.pop(record_id, None) is not None

    async def count(self, filters: DeadLetterFilter | None = None) -> int:
        async with self._lock:
            if filters is None:
                return len(self._records)
            return sum(filters.matches(record) for record in self._records.values())

    async def quarantine(self, record_id: str, *, reason: str) -> DeadLetterRecord:
        """Move an active record into quarantine."""

        if not reason:
            raise ValueError("quarantine reason must not be empty")
        async with self._lock:
            record = self._require_locked(record_id)
            if record.status is DeadLetterStatus.REPLAYED:
                raise ValueError("a replayed record cannot be quarantined")
            updated = replace(
                record,
                status=DeadLetterStatus.QUARANTINED,
                quarantine_reason=reason,
                updated_at=datetime.now(UTC),
            )
            self._records[record_id] = updated
            return updated

    async def release(self, record_id: str) -> DeadLetterRecord:
        """Release a record from quarantine."""

        async with self._lock:
            record = self._require_locked(record_id)
            if record.status is not DeadLetterStatus.QUARANTINED:
                raise ValueError("record is not quarantined")
            updated = replace(
                record,
                status=DeadLetterStatus.ACTIVE,
                quarantine_reason=None,
                updated_at=datetime.now(UTC),
            )
            self._records[record_id] = updated
            return updated

    async def archive(self, record_id: str, *, reason: str = "retention") -> DeadLetterRecord:
        """Archive a non-replayed record."""

        async with self._lock:
            record = self._require_locked(record_id)
            if record.status is DeadLetterStatus.REPLAYED:
                raise ValueError("a replayed record cannot be archived")
            updated = replace(
                record,
                status=DeadLetterStatus.ARCHIVED,
                archive_reason=reason,
                updated_at=datetime.now(UTC),
            )
            self._records[record_id] = updated
            return updated

    async def purge(
        self,
        filters: DeadLetterFilter | None = None,
        *,
        confirm: bool = False,
    ) -> int:
        """Delete selected records; an unfiltered purge requires confirmation."""

        if filters is None and not confirm:
            raise ValueError("unfiltered dead-letter purge requires confirm=True")
        selected = filters or DeadLetterFilter()
        async with self._lock:
            ids = [record.id for record in self._records.values() if selected.matches(record)]
            for record_id in ids:
                del self._records[record_id]
            return len(ids)

    async def export(
        self, filters: DeadLetterFilter | None = None
    ) -> tuple[Mapping[str, object], ...]:
        """Export portable, JSON-safe administrative records."""

        records = await self.query(filters)
        return tuple(self._export_record(record) for record in records)

    @staticmethod
    def _export_record(record: DeadLetterRecord) -> Mapping[str, object]:
        return {
            "id": record.id,
            "status": record.status.value,
            "route": record.route,
            "destination": record.destination,
            "headers": dict(record.headers),
            "metadata": dict(record.metadata),
            "error_type": record.error_type,
            "error_message": record.error_message,
            "traceback": record.traceback,
            "failure_classification": record.failure_classification,
            "event_type": record.event_type,
            "schema_version": record.schema_version,
            "serializer": record.serializer,
            "dead_lettered_at": record.dead_lettered_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
            "replay_count": record.replay_count,
            "replay_failures": record.replay_failures,
            "envelope_base64": (
                base64.b64encode(record.envelope_bytes).decode("ascii")
                if record.envelope_bytes is not None
                else None
            ),
        }

    def _require_locked(self, record_id: str) -> DeadLetterRecord:
        try:
            return self._records[record_id]
        except KeyError as error:
            raise KeyError(f"unknown dead-letter record {record_id!r}") from error


__all__ = ["DeadLetterStore", "MemoryDeadLetterStore"]
