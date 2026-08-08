from __future__ import annotations

import math

import pytest

from pyev.exceptions import SerializationError, UnsafeSerializationError
from pyev.serialization import (
    DeserializationContext,
    JsonSerializer,
    MessagePackSerializer,
    PickleSerializer,
    SerializationContext,
    SerializerRegistry,
    UnsafeSerializerWarning,
)


def test_json_serializer_round_trip_and_rejects_unsafe_numbers() -> None:
    serializer = JsonSerializer()
    encoded = serializer.encode({"name": "Léa", "items": [1, True]}, SerializationContext())
    assert serializer.decode(encoded, DeserializationContext()) == {
        "items": [1, True],
        "name": "Léa",
    }

    with pytest.raises(SerializationError):
        serializer.encode({"invalid": math.inf}, SerializationContext())
    with pytest.raises(SerializationError):
        serializer.decode(b'{"key":1,"key":2}', DeserializationContext())


def test_registry_resolves_names_and_content_types_consistently() -> None:
    registry = SerializerRegistry.with_defaults()
    assert registry.default.name == "json"
    assert registry.resolve(content_type="application/json; charset=utf-8").name == "json"
    assert registry.resolve(name="JSON", content_type="application/json").name == "json"
    with pytest.raises(SerializationError):
        registry.resolve(name="json", content_type="application/msgpack")


def test_pickle_requires_constructor_and_trust_opt_in() -> None:
    disabled = PickleSerializer()
    with pytest.raises(UnsafeSerializationError):
        disabled.encode({"value": 1}, SerializationContext())

    with pytest.warns(UnsafeSerializerWarning):
        enabled = PickleSerializer(allow_unsafe=True)
    data = enabled.encode({"value": 1}, SerializationContext())
    with pytest.raises(UnsafeSerializationError):
        enabled.decode(data, DeserializationContext())
    assert enabled.decode(data, DeserializationContext(trusted=True)) == {"value": 1}


def test_messagepack_serializer_is_lazy_and_optional() -> None:
    serializer = MessagePackSerializer()
    if not serializer.is_available():
        with pytest.raises(SerializationError, match="optional 'msgpack'"):
            serializer.encode({"value": 1}, SerializationContext())
        return
    encoded = serializer.encode({"value": 1}, SerializationContext())
    assert serializer.decode(encoded, DeserializationContext()) == {"value": 1}
