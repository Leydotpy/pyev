from __future__ import annotations

from dataclasses import dataclass

import pytest

from pymq.acknowledgements import AcknowledgementMode
from pymq.delivery import Delivery, DeliveryState
from pymq.envelope import Envelope
from pymq.exceptions import AcknowledgementError, InvalidStateTransitionError


@dataclass
class RecordingAcknowledgement:
    operation: str | None = None
    calls: int = 0
    fail: bool = False

    async def _record(self, operation: str) -> None:
        self.calls += 1
        if self.fail:
            raise RuntimeError("transport unavailable")
        self.operation = operation

    async def ack(self) -> None:
        await self._record("ack")

    async def nack(self, requeue: bool = True) -> None:
        await self._record("requeue" if requeue else "nack")

    async def reject(self) -> None:
        await self._record("reject")

    async def requeue(self) -> None:
        await self._record("requeue")

    async def defer(self, delay: float) -> None:
        await self._record(f"defer:{delay}")

    async def touch(self, extension: float) -> None:
        await self._record(f"touch:{extension}")


def make_delivery(adapter: RecordingAcknowledgement) -> Delivery[dict[str, int]]:
    envelope = Envelope.create({"value": 1}, type="tests.delivery")
    return Delivery({"value": 1}, envelope, acknowledgement=adapter)


async def test_ack_is_idempotent_and_conflicting_terminal_action_fails() -> None:
    adapter = RecordingAcknowledgement()
    delivery = make_delivery(adapter)
    await delivery.start_processing()
    await delivery.ack()
    await delivery.ack()

    assert adapter.calls == 1
    assert delivery.state is DeliveryState.ACKNOWLEDGED
    with pytest.raises(InvalidStateTransitionError):
        await delivery.reject()


async def test_adapter_failure_leaves_delivery_unacknowledged() -> None:
    adapter = RecordingAcknowledgement(fail=True)
    delivery = make_delivery(adapter)

    with pytest.raises(AcknowledgementError):
        await delivery.ack()
    assert delivery.state is DeliveryState.DELIVERED


async def test_requeue_and_defer_transitions() -> None:
    requeued_adapter = RecordingAcknowledgement()
    requeued = make_delivery(requeued_adapter)
    await requeued.nack(requeue=True)
    assert requeued.state is DeliveryState.REQUEUED
    assert requeued_adapter.operation == "requeue"

    deferred_adapter = RecordingAcknowledgement()
    deferred = make_delivery(deferred_adapter)
    await deferred.defer(delay=2.5)
    assert deferred.state is DeliveryState.DEFERRED
    assert deferred_adapter.operation == "defer:2.5"


async def test_acknowledgement_none_mode_is_explicitly_unsupported() -> None:
    adapter = RecordingAcknowledgement()
    delivery = Delivery(
        {},
        Envelope.create({}, type="tests.none"),
        acknowledgement=adapter,
        mode=AcknowledgementMode.NONE,
    )
    with pytest.raises(Exception, match="Unsupported capability"):
        await delivery.ack()
    assert adapter.calls == 0


def test_transport_metadata_is_exposed_read_only() -> None:
    delivery = Delivery(
        {},
        Envelope.create({}, type="tests.metadata"),
        transport_metadata={"engine": "memory"},
    )
    assert delivery.transport_metadata == {"engine": "memory"}
    assert delivery.native_metadata() is delivery.transport_metadata
    with pytest.raises(TypeError):
        delivery.transport_metadata["engine"] = "other"  # type: ignore[index]
