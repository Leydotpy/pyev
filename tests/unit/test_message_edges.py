"""Security and adapter edge cases for message normalization."""

from __future__ import annotations

import math
from collections import namedtuple
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import UUID

import pytest

from pymq.exceptions import MessageValidationError
from pymq.message import (
    EVENT_NAME_ATTRIBUTE,
    EVENT_VERSION_ATTRIBUTE,
    is_message,
    message_name,
    message_to_payload,
    message_version,
    to_json_value,
)


class Colour(Enum):
    RED = "red"


class PydanticLike:
    def model_dump(self, *, mode: str) -> object:
        assert mode == "json"
        return {"ok": True}


class BrokenPydanticLike:
    def model_dump(self, *, mode: str) -> object:
        raise RuntimeError(mode)


class AttrName:
    def __init__(self, name: object) -> None:
        self.name = name


class AttrsLike:
    __attrs_attrs__ = (AttrName("visible"), AttrName(42))

    def __init__(self) -> None:
        self.visible = "yes"


class DictionaryLike:
    def to_dict(self) -> object:
        return {"value": 3}


class PlainObject:
    def __init__(self) -> None:
        self.visible = "yes"
        self._private = "no"


def test_message_metadata_rejects_missing_and_boolean_versions() -> None:
    class Undeclared:
        pass

    class BooleanVersion:
        pass

    setattr(BooleanVersion, EVENT_NAME_ATTRIBUTE, "example.boolean")
    setattr(BooleanVersion, EVENT_VERSION_ATTRIBUTE, True)

    assert not is_message(Undeclared())
    assert not is_message(BooleanVersion())
    with pytest.raises(MessageValidationError, match="event name"):
        message_name(Undeclared())
    with pytest.raises(MessageValidationError, match="schema version"):
        message_version(BooleanVersion())


def test_message_model_adapters_produce_object_payloads() -> None:
    Point = namedtuple("Point", "x y")

    assert message_to_payload(PydanticLike()) == {"ok": True}
    assert message_to_payload(AttrsLike()) == {"visible": "yes"}
    assert message_to_payload(Point(1, 2)) == {"x": 1, "y": 2}
    assert message_to_payload(DictionaryLike()) == {"value": 3}
    assert message_to_payload(PlainObject()) == {"visible": "yes"}


def test_broken_or_non_object_models_fail_with_typed_errors() -> None:
    class ListModel:
        def to_dict(self) -> object:
            return [1, 2]

    with pytest.raises(MessageValidationError, match="could not be converted"):
        message_to_payload(BrokenPydanticLike())
    with pytest.raises(MessageValidationError, match="payload must be a JSON object"):
        message_to_payload(ListModel())
    with pytest.raises(MessageValidationError, match="mapping or a supported typed model"):
        message_to_payload(42)


def test_json_conversion_handles_safe_scalar_adapters() -> None:
    aware = datetime(2026, 8, 6, 12, tzinfo=UTC)
    identifier = UUID("00000000-0000-0000-0000-000000000001")
    converted = to_json_value(
        {
            "enum": Colour.RED,
            "datetime": aware,
            "date": date(2026, 8, 6),
            "time": time(12, 30),
            "uuid": identifier,
            "decimal": Decimal("1.25"),
            "path": Path("safe/file.txt"),
            "tuple": (1, 2),
        }
    )

    assert converted == {
        "enum": "red",
        "datetime": "2026-08-06T12:00:00Z",
        "date": "2026-08-06",
        "time": "12:30:00",
        "uuid": str(identifier),
        "decimal": "1.25",
        "path": str(Path("safe/file.txt")),
        "tuple": [1, 2],
    }


@pytest.mark.parametrize(
    "value",
    [b"binary", bytearray(b"binary"), memoryview(b"binary"), {1, 2}, object()],
)
def test_json_conversion_rejects_unsafe_or_nondeterministic_values(value: object) -> None:
    with pytest.raises(MessageValidationError):
        to_json_value(value)


def test_json_conversion_rejects_invalid_numbers_keys_depth_and_cycles() -> None:
    with pytest.raises(ValueError, match="max_depth"):
        to_json_value({}, max_depth=0)
    with pytest.raises(MessageValidationError, match="non-finite"):
        to_json_value(math.inf)
    with pytest.raises(MessageValidationError, match="keys must be strings"):
        to_json_value({1: "bad"})
    with pytest.raises(MessageValidationError, match="maximum nesting"):
        to_json_value({"one": {"two": 2}}, max_depth=1)

    recursive_mapping: dict[str, object] = {}
    recursive_mapping["self"] = recursive_mapping
    with pytest.raises(MessageValidationError, match="recursive mapping"):
        to_json_value(recursive_mapping)

    recursive_list: list[object] = []
    recursive_list.append(recursive_list)
    with pytest.raises(MessageValidationError, match="recursive sequence"):
        to_json_value(recursive_list)


def test_nested_model_dump_failure_is_classified() -> None:
    with pytest.raises(MessageValidationError, match="Model value"):
        to_json_value({"model": BrokenPydanticLike()})


def test_dataclass_values_are_normalized_recursively() -> None:
    @dataclass
    class Nested:
        count: int

    assert to_json_value(Nested(2)) == {"count": 2}
