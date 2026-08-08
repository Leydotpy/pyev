from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass
from datetime import UTC, datetime

import pytest

from pyev.envelope import Envelope
from pyev.event import EventRegistry, event
from pyev.exceptions import MessageValidationError, SerializationError


def test_envelope_message_and_wire_round_trip() -> None:
    registry = EventRegistry()

    @event("tests.order.placed", version=2, registry=registry)
    @dataclass(frozen=True, slots=True)
    class OrderPlaced:
        order_id: int
        labels: list[str]

    original = Envelope.from_message(
        OrderPlaced(42, ["priority"]),
        id="message-1",
        timestamp=datetime(2026, 8, 6, tzinfo=UTC),
        source="test-suite",
        correlation_id="correlation-1",
        headers={"tenant": "demo"},
    )

    reconstructed = Envelope.from_bytes(original.to_bytes())

    assert reconstructed == original
    assert reconstructed.to_message(registry=registry) == OrderPlaced(42, ["priority"])
    assert reconstructed.to_dict()["timestamp"] == "2026-08-06T00:00:00Z"


def test_envelope_is_deeply_immutable_and_returns_detached_mapping() -> None:
    payload = {"nested": {"values": [1, 2]}}
    envelope = Envelope.create(payload, type="tests.immutable")
    payload["nested"] = {}

    nested = envelope.payload["nested"]
    assert nested == {"values": (1, 2)}
    with pytest.raises(TypeError):
        envelope.payload["new"] = "value"  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        envelope.id = "replacement"  # type: ignore[misc]

    detached = envelope.to_dict()
    detached_payload = detached["payload"]
    assert isinstance(detached_payload, dict)
    detached_payload["nested"] = "changed"
    assert envelope.payload["nested"] == {"values": (1, 2)}


def test_envelope_rejects_naive_time_and_oversized_wire_data() -> None:
    with pytest.raises(MessageValidationError):
        Envelope.create({}, type="tests.time", timestamp=datetime(2026, 8, 6))

    envelope = Envelope.create({}, type="tests.size")
    with pytest.raises(SerializationError):
        Envelope.from_bytes(envelope.to_bytes(), max_size=1)


def test_envelope_rejects_ambiguous_duplicate_json_fields() -> None:
    duplicate = (
        b'{"id":"one","id":"two","type":"tests.duplicate","version":1,'
        b'"timestamp":"2026-08-06T00:00:00Z","payload":{}}'
    )
    with pytest.raises(SerializationError):
        Envelope.from_bytes(duplicate)
