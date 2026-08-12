"""Dead-letter capture, administration, and retention orchestration."""

from __future__ import annotations

import inspect
import traceback as traceback_module
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime

from pymq.exceptions import DeadLetterError
from pymq.observability.metrics import DEAD_LETTERS_TOTAL, MetricsProvider, NoOpMetrics
from pymq.observability.redaction import redact_mapping, redact_text, redact_value

from .models import (
    DeadLetterContext,
    DeadLetterFilter,
    DeadLetterPolicy,
    DeadLetterRecord,
    DeadLetterStatus,
)
from .store import DeadLetterStore


class DeadLetterManager:
    """Create sanitized records and coordinate administrative operations."""

    def __init__(
        self,
        store: DeadLetterStore,
        policy: DeadLetterPolicy | None = None,
        *,
        metrics: MetricsProvider | None = None,
        event_emitter: object | None = None,
    ) -> None:
        self.store = store
        self.policy = policy if policy is not None else DeadLetterPolicy()
        self.metrics = metrics if metrics is not None else NoOpMetrics()
        self._events = event_emitter

    async def dead_letter(
        self,
        envelope: object,
        error: BaseException,
        *,
        context: DeadLetterContext | None = None,
        policy: DeadLetterPolicy | None = None,
    ) -> DeadLetterRecord:
        """Persist one failure without exposing secrets or requiring a broker."""

        selected = policy if policy is not None else self.policy
        failure_context = context if context is not None else DeadLetterContext()
        now = datetime.now(UTC)
        envelope_bytes = failure_context.original_envelope_bytes
        if envelope_bytes is None:
            if isinstance(envelope, bytes):
                envelope_bytes = envelope
            else:
                to_bytes = getattr(envelope, "to_bytes", None)
                if callable(to_bytes):
                    try:
                        result = to_bytes()
                        if isinstance(result, bytes):
                            envelope_bytes = result
                    except Exception:
                        # The original failure is more important than an optional
                        # secondary representation. The object is still retained.
                        envelope_bytes = None

        envelope_headers = getattr(envelope, "headers", {})
        headers = {
            **(dict(envelope_headers) if isinstance(envelope_headers, Mapping) else {}),
            **dict(failure_context.headers),
        }
        headers = redact_mapping(headers, extra_keys=selected.sensitive_headers)
        metadata = redact_mapping(
            failure_context.metadata,
            extra_keys=selected.sensitive_headers,
        )
        formatted_traceback: str | None = None
        if selected.include_traceback and selected.traceback_limit:
            formatted_traceback = "".join(
                traceback_module.format_exception(type(error), error, error.__traceback__)
            )
            formatted_traceback = redact_text(formatted_traceback)[-selected.traceback_limit :]

        decoded_payload = None
        if selected.include_decoded_payload:
            decoded_payload = (
                failure_context.decoded_payload
                if failure_context.decoded_payload is not None
                else getattr(envelope, "payload", None)
            )
            decoded_payload = redact_value(
                decoded_payload,
                extra_keys=selected.sensitive_headers,
            )

        record = DeadLetterRecord(
            envelope=None if isinstance(envelope, bytes) else envelope,
            envelope_bytes=envelope_bytes,
            error_type=type(error).__name__,
            error_message=str(error),
            route=failure_context.route,
            destination=failure_context.destination,
            headers=headers,
            metadata=metadata,
            traceback=formatted_traceback,
            retry_history=failure_context.retry_history,
            first_failed_at=failure_context.first_failed_at or now,
            dead_lettered_at=now,
            updated_at=now,
            consumer=failure_context.consumer,
            subscription=failure_context.subscription,
            engine=failure_context.engine,
            failure_classification=failure_context.failure_classification,
            event_type=_string_attribute(envelope, "type"),
            schema_version=_integer_attribute(envelope, "version"),
            serializer=_string_attribute(envelope, "serializer"),
            decoded_payload=decoded_payload,
        )
        try:
            await self.store.put(record)
        except Exception as store_error:
            raise DeadLetterError(
                "failed to persist dead-letter record",
                retryable=True,
                context={
                    "error_type": type(store_error).__name__,
                    "route": failure_context.route,
                    "engine": failure_context.engine,
                },
            ) from store_error
        self.metrics.increment(
            DEAD_LETTERS_TOTAL,
            labels={
                "engine": failure_context.engine or "unknown",
                "classification": failure_context.failure_classification,
            },
        )
        await self._emit(
            "dead_lettered",
            record_id=record.id,
            route=record.route,
            engine=record.engine,
            error_type=record.error_type,
            classification=record.failure_classification,
        )
        return record

    async def inspect(self, record_id: str) -> DeadLetterRecord:
        """Return one record or raise an actionable error."""

        record = await self.store.get(record_id)
        if record is None:
            raise DeadLetterError(
                f"dead-letter record {record_id!r} does not exist",
                retryable=False,
                context={"record_id": record_id},
            )
        return record

    async def filter(self, filters: DeadLetterFilter | None = None) -> Sequence[DeadLetterRecord]:
        """Inspect records matching portable predicates."""

        return await self.store.query(filters)

    async def quarantine(self, record_id: str, *, reason: str) -> DeadLetterRecord:
        """Prevent replay of a suspicious or repeatedly failing record."""

        if not reason:
            raise ValueError("reason must not be empty")
        record = await self.inspect(record_id)
        if record.status is DeadLetterStatus.REPLAYED:
            raise DeadLetterError(
                "a replayed record cannot be quarantined", context={"record_id": record_id}
            )
        updated = replace(
            record,
            status=DeadLetterStatus.QUARANTINED,
            quarantine_reason=reason,
            updated_at=datetime.now(UTC),
        )
        await self.store.update(updated)
        return updated

    async def release(self, record_id: str) -> DeadLetterRecord:
        """Release a record from quarantine into the replayable set."""

        record = await self.inspect(record_id)
        if record.status is not DeadLetterStatus.QUARANTINED:
            raise DeadLetterError("record is not quarantined", context={"record_id": record_id})
        updated = replace(
            record,
            status=DeadLetterStatus.ACTIVE,
            quarantine_reason=None,
            updated_at=datetime.now(UTC),
        )
        await self.store.update(updated)
        return updated

    async def archive(self, record_id: str, *, reason: str = "manual") -> DeadLetterRecord:
        """Move a record out of the active administrative set."""

        record = await self.inspect(record_id)
        if record.status is DeadLetterStatus.REPLAYED:
            raise DeadLetterError(
                "a replayed record cannot be archived", context={"record_id": record_id}
            )
        updated = replace(
            record,
            status=DeadLetterStatus.ARCHIVED,
            archive_reason=reason,
            updated_at=datetime.now(UTC),
        )
        await self.store.update(updated)
        return updated

    async def purge(
        self,
        filters: DeadLetterFilter | None = None,
        *,
        confirm: bool = False,
    ) -> int:
        """Delete selected records; bulk deletion always requires confirmation."""

        records = tuple(await self.store.query(filters))
        if len(records) != 1 and not confirm:
            raise DeadLetterError(
                "purging zero or multiple records requires confirm=True",
                context={"selected": len(records)},
            )
        deleted = 0
        for record in records:
            deleted += int(await self.store.delete(record.id))
        return deleted

    async def apply_retention(self, *, now: datetime | None = None) -> tuple[int, int]:
        """Archive and purge records according to configured retention windows."""

        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        archived = 0
        purged = 0
        records = tuple(await self.store.query())
        for record in records:
            age = current - record.dead_lettered_at
            if self.policy.retention is not None and age >= self.policy.retention:
                purged += int(await self.store.delete(record.id))
            elif (
                self.policy.archive_after is not None
                and age >= self.policy.archive_after
                and record.status in (DeadLetterStatus.ACTIVE, DeadLetterStatus.QUARANTINED)
            ):
                await self.archive(record.id, reason="retention")
                archived += 1
        return archived, purged

    async def _emit(self, event_name: str, **details: object) -> None:
        emitter = self._events
        if emitter is None:
            return
        emit = getattr(emitter, "emit", None)
        if emit is None:
            return
        result = emit(event_name, **details)
        if inspect.isawaitable(result):
            await result


def _string_attribute(value: object, name: str) -> str | None:
    candidate = getattr(value, name, None)
    return candidate if isinstance(candidate, str) else None


def _integer_attribute(value: object, name: str) -> int | None:
    candidate = getattr(value, name, None)
    return candidate if isinstance(candidate, int) and not isinstance(candidate, bool) else None


__all__ = ["DeadLetterManager"]
