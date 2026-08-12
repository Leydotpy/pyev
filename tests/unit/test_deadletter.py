from __future__ import annotations

from datetime import timedelta

import pytest

from pymq.deadletter import (
    DeadLetterContext,
    DeadLetterFilter,
    DeadLetterManager,
    DeadLetterPolicy,
    DeadLetterStatus,
    MemoryDeadLetterStore,
    ReplayManager,
    ReplayOutcome,
)
from pymq.envelope import Envelope
from pymq.exceptions import DeadLetterError
from pymq.observability import REDACTED


def make_envelope(*, event_type: str = "orders.created") -> Envelope:
    return Envelope.create(
        {"order_id": 42},
        type=event_type,
        headers={"authorization": "Bearer secret", "tenant": "acme"},
    )


@pytest.mark.asyncio
async def test_dead_letter_capture_preserves_envelope_and_redacts_secrets() -> None:
    store = MemoryDeadLetterStore()
    manager = DeadLetterManager(store)
    try:
        raise RuntimeError("connection redis://user:password@example.test failed")
    except RuntimeError as error:
        record = await manager.dead_letter(
            make_envelope(),
            error,
            context=DeadLetterContext(route="orders.*", engine="memory"),
        )

    assert record.envelope_bytes is not None
    assert record.headers["authorization"] == REDACTED
    assert "user:password" not in record.error_message
    assert record.decoded_payload is None
    assert (await manager.inspect(record.id)).event_type == "orders.created"


@pytest.mark.asyncio
async def test_filter_quarantine_release_archive_and_safe_purge() -> None:
    store = MemoryDeadLetterStore()
    manager = DeadLetterManager(store)
    first = await manager.dead_letter(
        make_envelope(),
        ValueError("bad"),
        context=DeadLetterContext(route="orders.created", engine="memory"),
    )
    await manager.dead_letter(
        make_envelope(event_type="users.created"),
        ValueError("bad"),
        context=DeadLetterContext(route="users.created", engine="memory"),
    )

    matches = await manager.filter(DeadLetterFilter(route_pattern="orders.*"))
    assert [item.id for item in matches] == [first.id]
    assert (
        await manager.quarantine(first.id, reason="schema review")
    ).status is DeadLetterStatus.QUARANTINED
    assert (await manager.release(first.id)).status is DeadLetterStatus.ACTIVE
    assert (await manager.archive(first.id)).status is DeadLetterStatus.ARCHIVED

    with pytest.raises(DeadLetterError, match="confirm"):
        await manager.purge()
    assert await manager.purge(confirm=True) == 2


@pytest.mark.asyncio
async def test_replay_success_is_terminal_and_bulk_requires_confirmation() -> None:
    store = MemoryDeadLetterStore()
    manager = DeadLetterManager(store)
    record = await manager.dead_letter(make_envelope(), RuntimeError("failed"))
    published: list[tuple[object, str | None]] = []

    async def publish(envelope: object, destination: str | None) -> object:
        published.append((envelope, destination))
        return None

    replay = ReplayManager(store, publish)
    result = await replay.replay(record.id, destination_override="recovered")

    assert result.outcome is ReplayOutcome.SUCCEEDED
    assert published == [(record.envelope, "recovered")]
    assert (await replay.replay(record.id)).outcome is ReplayOutcome.SKIPPED

    second = await manager.dead_letter(make_envelope(), RuntimeError("failed"))
    with pytest.raises(DeadLetterError, match="confirm"):
        await replay.replay_selected([record.id, second.id])


@pytest.mark.asyncio
async def test_repeated_replay_failure_quarantines_record() -> None:
    store = MemoryDeadLetterStore()
    manager = DeadLetterManager(store)
    record = await manager.dead_letter(make_envelope(), RuntimeError("failed"))

    async def publish(envelope: object, destination: str | None) -> object:
        raise OSError("still unavailable")

    replay = ReplayManager(
        store,
        publish,
        DeadLetterPolicy(quarantine_after_replay_failures=2),
    )
    assert (await replay.replay(record.id)).outcome is ReplayOutcome.FAILED
    assert (await replay.replay(record.id)).outcome is ReplayOutcome.QUARANTINED
    assert (await store.get(record.id)).status is DeadLetterStatus.QUARANTINED  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_retention_archives_then_purges() -> None:
    store = MemoryDeadLetterStore()
    manager = DeadLetterManager(
        store,
        DeadLetterPolicy(archive_after=timedelta(days=1), retention=timedelta(days=2)),
    )
    record = await manager.dead_letter(make_envelope(), RuntimeError("failed"))
    archived, purged = await manager.apply_retention(
        now=record.dead_lettered_at + timedelta(days=1, seconds=1)
    )
    assert (archived, purged) == (1, 0)
    archived, purged = await manager.apply_retention(
        now=record.dead_lettered_at + timedelta(days=2, seconds=1)
    )
    assert (archived, purged) == (0, 1)
