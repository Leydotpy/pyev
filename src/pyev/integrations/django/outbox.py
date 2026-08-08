"""Django-backed transactional outbox foundation.

The store is opt-in: applications provide a concrete subclass of
``AbstractOutboxRecord`` and migrate it.  ``add_in_transaction`` is synchronous
on purpose so its insert uses the caller's active Django database transaction.
Dispatch remains asynchronous and uses the portable core ``OutboxDispatcher``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, cast
from uuid import UUID, uuid4

from pyev.envelope import Envelope
from pyev.exceptions import ConfigurationError, PyevError
from pyev.reliability.outbox import (
    OutboxDispatcher,
    OutboxMessage,
    OutboxStatus,
    OutboxStore,
)

if TYPE_CHECKING:
    from pyev.broker import Broker


class OutboxCodec(Protocol):
    """Encode and decode the envelope stored by a Django outbox row."""

    def encode(self, envelope: object) -> bytes: ...

    def decode(self, payload: bytes) -> object: ...


@dataclass(frozen=True, slots=True)
class EnvelopeOutboxCodec:
    """Canonical JSON codec for pyev :class:`Envelope` objects."""

    def encode(self, envelope: object) -> bytes:
        if not isinstance(envelope, Envelope):
            raise TypeError("the default Django outbox codec requires a pyev Envelope")
        return envelope.to_bytes()

    def decode(self, payload: bytes) -> Envelope:
        return Envelope.from_bytes(payload)


class DjangoModelOutboxStore:
    """Portable ``OutboxStore`` backed by an application-owned Django model.

    ``model`` must be a concrete subclass of ``AbstractOutboxRecord``.  Leasing
    uses ``SELECT FOR UPDATE`` and ``SKIP LOCKED`` where the selected database
    supports it; databases without ``SKIP LOCKED`` serialize lease acquisition.
    """

    def __init__(
        self,
        model: type[Any],
        *,
        codec: OutboxCodec | None = None,
        database: str = "default",
    ) -> None:
        if model._meta.abstract:
            raise TypeError("Django outbox model must be a concrete migrated model")
        self.model = model
        self.codec = codec or EnvelopeOutboxCodec()
        self.database = database

    def add_in_transaction(self, message: OutboxMessage) -> None:
        """Insert using the caller's current synchronous DB transaction."""

        self.model.objects.using(self.database).create(
            id=UUID(message.id),
            payload=self.codec.encode(message.envelope),
            destination=message.destination,
            status=message.status.value,
            created_at=message.created_at,
            available_at=message.available_at,
            attempts=message.attempts,
            last_error=message.last_error,
            lease_id=UUID(message.lease_id) if message.lease_id else None,
            leased_until=message.leased_until,
            published_at=message.published_at,
            metadata=dict(message.metadata),
        )

    async def add(self, message: OutboxMessage) -> None:
        """Insert outside a caller transaction; use ``add_in_transaction`` for atomicity."""

        from asgiref.sync import sync_to_async

        await sync_to_async(self.add_in_transaction, thread_sensitive=True)(message)

    async def get(self, message_id: str) -> OutboxMessage | None:
        from asgiref.sync import sync_to_async

        def fetch() -> OutboxMessage | None:
            row = self.model.objects.using(self.database).filter(pk=message_id).first()
            return None if row is None else self._from_row(row)

        return await sync_to_async(fetch, thread_sensitive=True)()

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

        from asgiref.sync import sync_to_async

        return await sync_to_async(self._lease_sync, thread_sensitive=True)(
            limit,
            lease_duration,
            current,
        )

    def _lease_sync(
        self,
        limit: int,
        lease_duration: float,
        current: datetime,
    ) -> tuple[OutboxMessage, ...]:
        from datetime import timedelta

        from django.db import connections, transaction

        manager = self.model.objects.using(self.database)
        connection = connections[self.database]
        with transaction.atomic(using=self.database):
            manager.filter(
                status=OutboxStatus.LEASED.value,
                leased_until__lte=current,
            ).update(
                status=OutboxStatus.FAILED.value,
                lease_id=None,
                leased_until=None,
            )
            query = manager.filter(
                status__in=(OutboxStatus.PENDING.value, OutboxStatus.FAILED.value),
                available_at__lte=current,
            ).order_by("available_at", "created_at", "id")
            if connection.features.has_select_for_update_skip_locked:
                query = query.select_for_update(skip_locked=True)
            else:
                query = query.select_for_update()
            rows = list(query[:limit])
            leased: list[OutboxMessage] = []
            for row in rows:
                row.status = OutboxStatus.LEASED.value
                row.lease_id = uuid4()
                row.leased_until = current + timedelta(seconds=lease_duration)
                row.attempts += 1
                row.save(
                    using=self.database,
                    update_fields=("status", "lease_id", "leased_until", "attempts"),
                )
                leased.append(self._from_row(row))
            return tuple(leased)

    async def mark_published(self, message_id: str, lease_id: str) -> None:
        from asgiref.sync import sync_to_async

        def update() -> int:
            return cast(
                int,
                self.model.objects.using(self.database)
                .filter(
                    pk=message_id,
                    lease_id=lease_id,
                    status=OutboxStatus.LEASED.value,
                )
                .update(
                    status=OutboxStatus.PUBLISHED.value,
                    published_at=datetime.now(UTC),
                    lease_id=None,
                    leased_until=None,
                    last_error=None,
                ),
            )

        if await sync_to_async(update, thread_sensitive=True)() != 1:
            raise ValueError("outbox lease no longer belongs to this dispatcher")

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
        from asgiref.sync import sync_to_async

        def update() -> int:
            values: dict[str, object] = {
                "status": (
                    OutboxStatus.DEAD_LETTERED.value if terminal else OutboxStatus.FAILED.value
                ),
                "last_error": error,
                "lease_id": None,
                "leased_until": None,
            }
            if retry_at is not None:
                values["available_at"] = retry_at
            return cast(
                int,
                self.model.objects.using(self.database)
                .filter(
                    pk=message_id,
                    lease_id=lease_id,
                    status=OutboxStatus.LEASED.value,
                )
                .update(**values),
            )

        if await sync_to_async(update, thread_sensitive=True)() != 1:
            raise ValueError("outbox lease no longer belongs to this dispatcher")

    async def list(
        self,
        *,
        status: OutboxStatus | None = None,
        limit: int = 100,
    ) -> tuple[OutboxMessage, ...]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        from asgiref.sync import sync_to_async

        def fetch() -> tuple[OutboxMessage, ...]:
            query = self.model.objects.using(self.database).order_by(
                "available_at", "created_at", "id"
            )
            if status is not None:
                query = query.filter(status=status.value)
            return tuple(self._from_row(row) for row in query[:limit])

        return await sync_to_async(fetch, thread_sensitive=True)()

    async def purge_published(self, *, before: datetime) -> int:
        if before.tzinfo is None:
            raise ValueError("before must be timezone-aware")
        from asgiref.sync import sync_to_async

        def purge() -> int:
            deleted, _ = (
                self.model.objects.using(self.database)
                .filter(
                    status=OutboxStatus.PUBLISHED.value,
                    published_at__lt=before,
                )
                .delete()
            )
            return cast(int, deleted)

        return await sync_to_async(purge, thread_sensitive=True)()

    def _from_row(self, row: Any) -> OutboxMessage:
        return OutboxMessage(
            id=str(row.id),
            envelope=self.codec.decode(bytes(row.payload)),
            destination=str(row.destination),
            status=OutboxStatus(row.status),
            created_at=row.created_at,
            available_at=row.available_at,
            attempts=int(row.attempts),
            last_error=row.last_error,
            lease_id=str(row.lease_id) if row.lease_id else None,
            leased_until=row.leased_until,
            published_at=row.published_at,
            metadata=cast(Mapping[str, object], row.metadata or {}),
        )


_outbox_store: OutboxStore | None = None


def configure_outbox_store(store: OutboxStore | None) -> None:
    """Configure the outbox store owned by this Django process."""

    global _outbox_store
    _outbox_store = store


def get_outbox_store(*, required: bool = True) -> OutboxStore | None:
    """Resolve ``PYEV.DJANGO.OUTBOX_STORE`` lazily for this process."""

    global _outbox_store
    if _outbox_store is None:
        from django.utils.module_loading import import_string

        from pyev.integrations.django.config import DjangoSettingsProvider

        configured = DjangoSettingsProvider().option("django.outbox_store")
        if isinstance(configured, str):
            configured = import_string(configured)
        if isinstance(configured, type):
            configured = configured()
        elif callable(configured) and not isinstance(configured, OutboxStore):
            configured = configured()
        if configured is not None and not isinstance(configured, OutboxStore):
            raise ConfigurationError("PYEV.DJANGO.OUTBOX_STORE must provide OutboxStore methods")
        _outbox_store = configured
    if required and _outbox_store is None:
        raise ConfigurationError(
            "no Django outbox store is configured; set PYEV.DJANGO.OUTBOX_STORE"
        )
    return _outbox_store


def outbox_publisher(
    broker: Broker,
) -> Callable[[OutboxMessage], Awaitable[None]]:
    """Build an ``OutboxDispatcher`` callback using a running broker."""

    async def publish(message: OutboxMessage) -> None:
        envelope = message.envelope
        route = message.destination
        headers: Mapping[str, str] | None = None
        if isinstance(envelope, Envelope):
            headers = envelope.headers
            route = route or envelope.type
            try:
                value = envelope.to_message(registry=broker.event_registry)
            except PyevError:
                value = dict(envelope.payload)
        else:
            value = envelope
        await broker.publish(value, route=route, headers=headers)

    return publish


def build_outbox_dispatcher(
    broker: Broker,
    *,
    store: OutboxStore | None = None,
) -> OutboxDispatcher:
    """Create a bounded dispatcher from Django settings."""

    from pyev.integrations.django.config import DjangoSettingsProvider

    provider = DjangoSettingsProvider()
    selected = store or get_outbox_store()
    assert selected is not None
    return OutboxDispatcher(
        selected,
        outbox_publisher(broker),
        batch_size=_integer_option(
            provider.option("django.outbox_batch_size", 100),
            "PYEV.DJANGO.OUTBOX_BATCH_SIZE",
        ),
        lease_duration=_number_option(
            provider.option("django.outbox_lease_duration", 30.0),
            "PYEV.DJANGO.OUTBOX_LEASE_DURATION",
        ),
        max_attempts=_integer_option(
            provider.option("django.outbox_max_attempts", 10),
            "PYEV.DJANGO.OUTBOX_MAX_ATTEMPTS",
        ),
        idle_delay=_number_option(
            provider.option("django.outbox_idle_delay", 1.0),
            "PYEV.DJANGO.OUTBOX_IDLE_DELAY",
        ),
    )


def _integer_option(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ConfigurationError(f"{name} must be an integer")
    try:
        return int(value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error


def _number_option(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ConfigurationError(f"{name} must be a number")
    try:
        return float(value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a number") from error


__all__ = [
    "DjangoModelOutboxStore",
    "EnvelopeOutboxCodec",
    "OutboxCodec",
    "build_outbox_dispatcher",
    "configure_outbox_store",
    "get_outbox_store",
    "outbox_publisher",
]
