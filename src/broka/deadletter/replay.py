"""Safeguarded dead-letter replay and quarantine workflows."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum

from broka.exceptions import DeadLetterError

from .manager import DeadLetterManager
from .models import DeadLetterFilter, DeadLetterPolicy, DeadLetterRecord, DeadLetterStatus
from .store import DeadLetterStore

type ReplayPublisher = Callable[[object, str | None], Awaitable[object]]
type ReplayDecoder = Callable[[bytes], object]


class ReplayOutcome(StrEnum):
    """Result category for an attempted replay."""

    DRY_RUN = "dry_run"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Outcome of replaying one record."""

    record_id: str
    outcome: ReplayOutcome
    destination: str | None
    replay_count: int
    error: str | None = None


class ReplayManager:
    """Replay dead letters with caps, rate limiting, and quarantine safety."""

    def __init__(
        self,
        store: DeadLetterStore,
        publish: ReplayPublisher,
        policy: DeadLetterPolicy | None = None,
        *,
        decoder: ReplayDecoder | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        event_emitter: object | None = None,
    ) -> None:
        self.store = store
        self._publish = publish
        self.policy = policy or DeadLetterPolicy()
        self._decoder = decoder
        self._sleep = sleep
        self._events = event_emitter

    async def replay(
        self,
        record_id: str,
        *,
        destination_override: str | None = None,
        dry_run: bool = False,
        include_quarantined: bool = False,
        include_archived: bool = False,
    ) -> ReplayResult:
        """Replay one record when administrative safeguards permit it."""

        record = await self.store.get(record_id)
        if record is None:
            raise DeadLetterError(
                f"dead-letter record {record_id!r} does not exist",
                context={"record_id": record_id},
            )
        destination = destination_override or record.destination
        if record.status is DeadLetterStatus.REPLAYED:
            return ReplayResult(
                record.id,
                ReplayOutcome.SKIPPED,
                destination,
                record.replay_count,
                "already replayed",
            )
        if record.status is DeadLetterStatus.QUARANTINED and not include_quarantined:
            return ReplayResult(
                record.id,
                ReplayOutcome.SKIPPED,
                destination,
                record.replay_count,
                "record is quarantined",
            )
        if record.status is DeadLetterStatus.ARCHIVED and not include_archived:
            return ReplayResult(
                record.id,
                ReplayOutcome.SKIPPED,
                destination,
                record.replay_count,
                "record is archived",
            )
        if record.replay_count >= self.policy.max_replay_attempts:
            updated = await self._quarantine(record, "maximum replay attempts reached")
            return ReplayResult(
                record.id,
                ReplayOutcome.QUARANTINED,
                destination,
                updated.replay_count,
                updated.quarantine_reason,
            )
        if dry_run:
            return ReplayResult(record.id, ReplayOutcome.DRY_RUN, destination, record.replay_count)

        envelope = record.envelope
        if envelope is None and record.envelope_bytes is not None and self._decoder is not None:
            envelope = self._decoder(record.envelope_bytes)
        if envelope is None:
            raise DeadLetterError(
                "record has no replayable envelope; configure a decoder",
                context={"record_id": record.id},
            )
        attempted_at = datetime.now(UTC)
        try:
            await self._publish(envelope, destination)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failures = record.replay_failures + 1
            status = (
                DeadLetterStatus.QUARANTINED
                if failures >= self.policy.quarantine_after_replay_failures
                else record.status
            )
            updated = replace(
                record,
                status=status,
                quarantine_reason=(
                    "repeated replay failure"
                    if status is DeadLetterStatus.QUARANTINED
                    else record.quarantine_reason
                ),
                replay_count=record.replay_count + 1,
                replay_failures=failures,
                last_replay_at=attempted_at,
                last_replay_error=f"{type(error).__name__}: {error}",
                updated_at=attempted_at,
            )
            await self.store.update(updated)
            outcome = (
                ReplayOutcome.QUARANTINED
                if updated.status is DeadLetterStatus.QUARANTINED
                else ReplayOutcome.FAILED
            )
            return ReplayResult(
                record.id,
                outcome,
                destination,
                updated.replay_count,
                updated.last_replay_error,
            )

        updated = replace(
            record,
            status=DeadLetterStatus.REPLAYED,
            replay_count=record.replay_count + 1,
            last_replay_at=attempted_at,
            last_replay_error=None,
            updated_at=attempted_at,
        )
        await self.store.update(updated)
        await self._emit(
            "replayed",
            record_id=record.id,
            destination=destination,
            replay_count=updated.replay_count,
        )
        return ReplayResult(record.id, ReplayOutcome.SUCCEEDED, destination, updated.replay_count)

    async def replay_selected(
        self,
        record_ids: Sequence[str],
        *,
        destination_override: str | None = None,
        dry_run: bool = False,
        confirm: bool = False,
        include_quarantined: bool = False,
        include_archived: bool = False,
    ) -> tuple[ReplayResult, ...]:
        """Replay a bounded explicit set, requiring confirmation for multiples."""

        ids = tuple(dict.fromkeys(record_ids))
        if len(ids) > self.policy.max_replay_batch:
            raise DeadLetterError(
                "replay selection exceeds configured batch limit",
                context={"selected": len(ids), "limit": self.policy.max_replay_batch},
            )
        if len(ids) > 1 and not (confirm or dry_run):
            raise DeadLetterError(
                "replaying multiple records requires confirm=True",
                context={"selected": len(ids)},
            )
        results: list[ReplayResult] = []
        interval = (
            1.0 / self.policy.replay_rate_limit
            if self.policy.replay_rate_limit is not None
            else None
        )
        for index, record_id in enumerate(ids):
            results.append(
                await self.replay(
                    record_id,
                    destination_override=destination_override,
                    dry_run=dry_run,
                    include_quarantined=include_quarantined,
                    include_archived=include_archived,
                )
            )
            if interval is not None and index + 1 < len(ids) and not dry_run:
                await self._sleep(interval)
        return tuple(results)

    async def replay_all(
        self,
        filters: DeadLetterFilter | None = None,
        *,
        destination_override: str | None = None,
        dry_run: bool = False,
        confirm: bool = False,
    ) -> tuple[ReplayResult, ...]:
        """Replay a filtered active set; execution requires explicit confirmation."""

        if not (confirm or dry_run):
            raise DeadLetterError("bulk replay requires confirm=True")
        selected_filter = filters or DeadLetterFilter(
            statuses=frozenset({DeadLetterStatus.ACTIVE}),
            limit=self.policy.max_replay_batch,
        )
        records = tuple(await self.store.query(selected_filter))
        if len(records) > self.policy.max_replay_batch:
            raise DeadLetterError(
                "bulk replay exceeds configured batch limit",
                context={"selected": len(records), "limit": self.policy.max_replay_batch},
            )
        return await self.replay_selected(
            [record.id for record in records],
            destination_override=destination_override,
            dry_run=dry_run,
            confirm=True,
        )

    async def _quarantine(self, record: DeadLetterRecord, reason: str) -> DeadLetterRecord:
        updated = replace(
            record,
            status=DeadLetterStatus.QUARANTINED,
            quarantine_reason=reason,
            updated_at=datetime.now(UTC),
        )
        await self.store.update(updated)
        return updated

    async def _emit(self, event_name: str, **details: object) -> None:
        emitter = self._events
        emit = getattr(emitter, "emit", None) if emitter is not None else None
        if emit is not None:
            result = emit(event_name, **details)
            if inspect.isawaitable(result):
                await result


class QuarantineManager:
    """Focused administrative view over dead-letter quarantine operations."""

    def __init__(self, manager: DeadLetterManager) -> None:
        self.manager = manager

    async def quarantine(self, record_id: str, *, reason: str) -> DeadLetterRecord:
        return await self.manager.quarantine(record_id, reason=reason)

    async def release(self, record_id: str) -> DeadLetterRecord:
        return await self.manager.release(record_id)

    async def list(self) -> Sequence[DeadLetterRecord]:
        return await self.manager.filter(DeadLetterFilter(quarantined=True))


__all__ = [
    "QuarantineManager",
    "ReplayDecoder",
    "ReplayManager",
    "ReplayOutcome",
    "ReplayPublisher",
    "ReplayResult",
]
