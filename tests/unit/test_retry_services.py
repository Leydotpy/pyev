from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pymq.reliability import MemoryIdempotencyStore, MemoryOutboxStore, OutboxMessage
from pymq.reliability.outbox import OutboxStatus
from pymq.testing import DeterministicClock


@pytest.mark.asyncio
async def test_memory_idempotency_claim_complete_and_expiry() -> None:
    clock = DeterministicClock()
    store = MemoryIdempotencyStore(default_ttl=10, processing_ttl=2, clock=clock)

    assert await store.claim("message-1")
    assert not await store.claim("message-1")
    await store.complete("message-1")
    assert await store.contains("message-1", completed_only=True)
    clock.advance(11)
    assert await store.get("message-1") is None


@pytest.mark.asyncio
async def test_memory_outbox_enforces_lease_ownership() -> None:
    store = MemoryOutboxStore()
    message = OutboxMessage(envelope=b"payload", destination="events")
    await store.add(message)
    leased = await store.lease(limit=1, lease_duration=10, now=datetime.now(UTC))
    assert leased[0].status is OutboxStatus.LEASED
    assert leased[0].lease_id is not None

    with pytest.raises(ValueError, match="lease"):
        await store.mark_published(message.id, "wrong")
    await store.mark_published(message.id, leased[0].lease_id)
    assert (await store.get(message.id)).status is OutboxStatus.PUBLISHED  # type: ignore[union-attr]
