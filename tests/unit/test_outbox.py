"""Transactional outbox storage and dispatcher behavior."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from pymq.reliability import (
    FixedBackoff,
    MemoryOutboxStore,
    OutboxDispatcher,
    OutboxMessage,
    OutboxStatus,
)


def _message(identifier: str, *, destination: str = "orders.created") -> OutboxMessage:
    now = datetime.now(UTC)
    return OutboxMessage(
        id=identifier,
        envelope={"id": identifier},
        destination=destination,
        created_at=now,
        available_at=now,
    )


def test_outbox_models_and_dispatcher_options_validate_early() -> None:
    with pytest.raises(ValueError, match="id"):
        replace(_message("valid"), id="")
    with pytest.raises(ValueError, match="destination"):
        replace(_message("valid"), destination="")
    with pytest.raises(ValueError, match="attempts"):
        replace(_message("valid"), attempts=-1)
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(_message("valid"), available_at=datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="max_entries"):
        MemoryOutboxStore(max_entries=0)

    async def publish(_message: OutboxMessage) -> None:
        return None

    store = MemoryOutboxStore()
    with pytest.raises(ValueError, match="at least 1"):
        OutboxDispatcher(store, publish, batch_size=0)
    with pytest.raises(ValueError, match="positive"):
        OutboxDispatcher(store, publish, lease_duration=0)


async def test_memory_outbox_capacity_leasing_and_lease_ownership() -> None:
    store = MemoryOutboxStore(max_entries=1)
    message = _message("one")
    await store.add(message)
    with pytest.raises(ValueError, match="already exists"):
        await store.add(message)
    with pytest.raises(OverflowError, match="capacity"):
        await store.add(_message("two"))
    with pytest.raises(ValueError, match="limit"):
        await store.lease(limit=0, lease_duration=1)
    with pytest.raises(ValueError, match="lease_duration"):
        await store.lease(limit=1, lease_duration=0)
    with pytest.raises(ValueError, match="timezone-aware"):
        await store.lease(limit=1, lease_duration=1, now=datetime(2026, 1, 1))

    leased = tuple(await store.lease(limit=1, lease_duration=10))
    assert len(leased) == 1
    assert leased[0].attempts == 1
    assert leased[0].lease_id is not None
    with pytest.raises(KeyError, match="unknown"):
        await store.mark_published("missing", "lease")
    with pytest.raises(ValueError, match="lease"):
        await store.mark_published("one", "wrong")
    with pytest.raises(ValueError, match="timezone-aware"):
        await store.mark_failed(
            "one",
            leased[0].lease_id,
            error="failed",
            retry_at=datetime(2026, 1, 1),
        )

    await store.mark_published("one", leased[0].lease_id)
    published = await store.get("one")
    assert published is not None and published.status is OutboxStatus.PUBLISHED


async def test_expired_leases_are_recovered_and_filtering_is_deterministic() -> None:
    store = MemoryOutboxStore()
    start = datetime.now(UTC)
    first = replace(_message("one"), created_at=start, available_at=start)
    second = replace(
        _message("two"),
        created_at=start + timedelta(microseconds=1),
        available_at=start,
    )
    await store.add(first)
    await store.add(second)
    leased = tuple(await store.lease(limit=1, lease_duration=1, now=start))
    assert [item.id for item in leased] == ["one"]

    recovered = tuple(
        await store.lease(limit=2, lease_duration=1, now=start + timedelta(seconds=2))
    )
    assert [item.id for item in recovered] == ["one", "two"]
    assert [item.id for item in await store.list(status=OutboxStatus.LEASED)] == [
        "one",
        "two",
    ]


async def test_dispatch_once_marks_success_and_terminal_failure() -> None:
    store = MemoryOutboxStore()
    await store.add(_message("ok"))
    await store.add(_message("bad", destination="orders.failed"))

    async def publish(message: OutboxMessage) -> None:
        if message.id == "bad":
            raise RuntimeError("password=secret")

    dispatcher = OutboxDispatcher(
        store,
        publish,
        max_attempts=1,
        backoff=FixedBackoff(0),
    )
    assert await dispatcher.dispatch_once() == 2
    successful = await store.get("ok")
    failed = await store.get("bad")
    assert successful is not None and successful.status is OutboxStatus.PUBLISHED
    assert failed is not None and failed.status is OutboxStatus.DEAD_LETTERED
    assert failed.last_error is not None and "secret" not in failed.last_error

    future = datetime.now(UTC) + timedelta(seconds=1)
    assert await store.purge_published(before=future) == 1
    assert await store.get("ok") is None
    with pytest.raises(ValueError, match="timezone-aware"):
        await store.purge_published(before=datetime(2026, 1, 1))


async def test_dispatcher_releases_lease_when_cancelled() -> None:
    store = MemoryOutboxStore()
    await store.add(_message("cancel"))

    async def cancel(_message: OutboxMessage) -> None:
        raise asyncio.CancelledError

    dispatcher = OutboxDispatcher(store, cancel)
    with pytest.raises(asyncio.CancelledError):
        await dispatcher.dispatch_once()
    message = await store.get("cancel")
    assert message is not None
    assert message.status is OutboxStatus.FAILED
    assert message.lease_id is None


async def test_dispatcher_run_waits_only_when_no_work() -> None:
    store = MemoryOutboxStore()
    stop = asyncio.Event()
    sleeps: list[float] = []

    async def publish(_message: OutboxMessage) -> None:
        return None

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        stop.set()

    dispatcher = OutboxDispatcher(store, publish, idle_delay=0.25, sleep=sleep)
    await dispatcher.run(stop)
    assert sleeps == [0.25]
