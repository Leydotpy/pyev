"""Administrative dead-letter storage and replay safeguards."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from pymq.deadletter import (
    DeadLetterFilter,
    DeadLetterPolicy,
    DeadLetterRecord,
    DeadLetterStatus,
    MemoryDeadLetterStore,
    ReplayManager,
    ReplayOutcome,
)
from pymq.exceptions import DeadLetterError


def _record(
    identifier: str,
    *,
    route: str = "orders.created",
    envelope: object | None = None,
    envelope_bytes: bytes | None = b"wire",
    status: DeadLetterStatus = DeadLetterStatus.ACTIVE,
    replay_count: int = 0,
) -> DeadLetterRecord:
    return DeadLetterRecord(
        id=identifier,
        envelope=envelope,
        envelope_bytes=envelope_bytes,
        error_type="RuntimeError",
        error_message="failed with password=secret",
        route=route,
        destination=route,
        engine="memory",
        event_type=route,
        schema_version=1,
        serializer="json",
        headers={"authorization": "Bearer secret", "safe": "visible"},
        status=status,
        replay_count=replay_count,
    )


async def test_memory_store_enforces_capacity_identity_and_updates() -> None:
    with pytest.raises(ValueError, match="max_entries"):
        MemoryDeadLetterStore(max_entries=0)

    store = MemoryDeadLetterStore(max_entries=1)
    record = _record("one")
    await store.put(record)
    with pytest.raises(ValueError, match="already exists"):
        await store.put(record)
    with pytest.raises(OverflowError, match="capacity"):
        await store.put(_record("two"))
    with pytest.raises(KeyError, match="unknown"):
        await store.update(_record("missing"))

    updated = replace(record, error_message="different")
    await store.update(updated)
    assert (await store.get("one")) == updated
    assert await store.count() == 1
    assert not await store.delete("missing")
    assert await store.delete("one")
    assert await store.count() == 0


async def test_store_filter_quarantine_archive_export_and_purge() -> None:
    store = MemoryDeadLetterStore()
    first = _record("one", envelope_bytes=b"first")
    second = _record("two", route="users.invited", envelope_bytes=None)
    await store.put(first)
    await store.put(second)

    selected = await store.inspect(DeadLetterFilter(route_pattern="orders.*", limit=1))
    assert [record.id for record in selected] == ["one"]
    assert await store.count(DeadLetterFilter(engine="memory")) == 2

    with pytest.raises(ValueError, match="reason"):
        await store.quarantine("one", reason="")
    quarantined = await store.quarantine("one", reason="manual review")
    assert quarantined.status is DeadLetterStatus.QUARANTINED
    assert (await store.release("one")).status is DeadLetterStatus.ACTIVE
    with pytest.raises(ValueError, match="not quarantined"):
        await store.release("one")

    archived = await store.archive("one", reason="retention")
    assert archived.archive_reason == "retention"
    exported = await store.export(DeadLetterFilter(ids=frozenset({"one"})))
    assert exported[0]["envelope_base64"] == "Zmlyc3Q="
    assert "secret" not in str(exported[0])

    with pytest.raises(ValueError, match="confirm=True"):
        await store.purge()
    assert await store.purge(DeadLetterFilter(ids=frozenset({"one"}))) == 1
    assert await store.purge(confirm=True) == 1
    with pytest.raises(KeyError, match="unknown"):
        await store.archive("missing")


async def test_store_rejects_invalid_terminal_administration() -> None:
    store = MemoryDeadLetterStore()
    replayed = _record("done", status=DeadLetterStatus.REPLAYED)
    await store.put(replayed)
    with pytest.raises(ValueError, match="replayed"):
        await store.quarantine("done", reason="late")
    with pytest.raises(ValueError, match="replayed"):
        await store.archive("done")


async def test_replay_manager_success_dry_run_decode_and_status_safeguards() -> None:
    store = MemoryDeadLetterStore()
    await store.put(_record("decode", envelope=None, envelope_bytes=b"encoded"))
    await store.put(_record("quarantine", status=DeadLetterStatus.QUARANTINED))
    await store.put(_record("archive", status=DeadLetterStatus.ARCHIVED))
    await store.put(_record("done", status=DeadLetterStatus.REPLAYED, replay_count=1))
    published: list[tuple[object, str | None]] = []

    async def publish(envelope: object, destination: str | None) -> None:
        published.append((envelope, destination))

    manager = ReplayManager(store, publish, decoder=lambda value: value.decode("ascii"))
    dry = await manager.replay("decode", dry_run=True)
    assert dry.outcome is ReplayOutcome.DRY_RUN
    result = await manager.replay("decode", destination_override="recovered")
    assert result.outcome is ReplayOutcome.SUCCEEDED
    assert published == [("encoded", "recovered")]
    assert (await manager.replay("done")).outcome is ReplayOutcome.SKIPPED
    assert (await manager.replay("quarantine")).outcome is ReplayOutcome.SKIPPED
    assert (await manager.replay("archive")).outcome is ReplayOutcome.SKIPPED

    with pytest.raises(DeadLetterError, match="does not exist"):
        await manager.replay("missing")


async def test_replay_failure_quarantines_and_missing_payload_fails() -> None:
    store = MemoryDeadLetterStore()
    await store.put(_record("failure", envelope={"value": 1}))
    await store.put(_record("empty", envelope=None, envelope_bytes=None))

    async def fail(_envelope: object, _destination: str | None) -> None:
        raise RuntimeError("token=secret")

    manager = ReplayManager(
        store,
        fail,
        DeadLetterPolicy(quarantine_after_replay_failures=1),
    )
    result = await manager.replay("failure")
    assert result.outcome is ReplayOutcome.QUARANTINED
    assert "secret" not in (result.error or "")
    with pytest.raises(DeadLetterError, match="no replayable envelope"):
        await manager.replay("empty")


async def test_bulk_replay_requires_confirmation_limits_and_rate_limits() -> None:
    store = MemoryDeadLetterStore()
    await store.put(_record("one", envelope={"id": 1}))
    await store.put(_record("two", envelope={"id": 2}))
    sleeps: list[float] = []

    async def publish(_envelope: object, _destination: str | None) -> None:
        return None

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    manager = ReplayManager(
        store,
        publish,
        DeadLetterPolicy(max_replay_batch=2, replay_rate_limit=4),
        sleep=sleep,
    )
    with pytest.raises(DeadLetterError, match="requires confirm"):
        await manager.replay_selected(["one", "two"])
    with pytest.raises(DeadLetterError, match="bulk replay"):
        await manager.replay_all()
    results = await manager.replay_selected(["one", "two", "two"], confirm=True)
    assert [result.outcome for result in results] == [
        ReplayOutcome.SUCCEEDED,
        ReplayOutcome.SUCCEEDED,
    ]
    assert sleeps == [0.25]

    limited = ReplayManager(
        store,
        publish,
        DeadLetterPolicy(max_replay_batch=1),
    )
    with pytest.raises(DeadLetterError, match="batch limit"):
        await limited.replay_selected(["one", "two"], dry_run=True)


async def test_replay_attempt_cap_moves_record_to_quarantine() -> None:
    store = MemoryDeadLetterStore()
    await store.put(_record("capped", envelope={}, replay_count=1))

    async def publish(_envelope: object, _destination: str | None) -> None:
        raise AssertionError("must not publish")

    manager = ReplayManager(store, publish, DeadLetterPolicy(max_replay_attempts=1))
    result = await manager.replay("capped")
    assert result.outcome is ReplayOutcome.QUARANTINED
    assert (await store.get("capped")).status is DeadLetterStatus.QUARANTINED  # type: ignore[union-attr]


def test_dead_letter_models_validate_time_and_policy_bounds() -> None:
    with pytest.raises(ValueError, match="limit"):
        DeadLetterFilter(limit=0)
    with pytest.raises(ValueError, match="timezone-aware"):
        DeadLetterFilter(created_after=datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="positive"):
        DeadLetterPolicy(replay_rate_limit=0)
    with pytest.raises(ValueError, match="cannot exceed"):
        DeadLetterPolicy(archive_after=timedelta(days=2), retention=timedelta(days=1))
