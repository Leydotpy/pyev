"""Message inspection and safe payload-normalization helpers."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path, PurePath
from typing import Any, Protocol, TypeAlias, cast, runtime_checkable
from uuid import UUID

from .exceptions import MessageValidationError
from .typing import JSONValue

EVENT_NAME_ATTRIBUTE = "__pyev_event_name__"
EVENT_VERSION_ATTRIBUTE = "__pyev_event_version__"
EVENT_METADATA_ATTRIBUTE = "__pyev_event_metadata__"


@runtime_checkable
class Message(Protocol):
    """Structural protocol implemented by classes decorated with ``@event``."""

    __pyev_event_name__: str
    __pyev_event_version__: int


MessageLike: TypeAlias = object


def is_message(value: object) -> bool:
    """Return whether ``value`` exposes valid pyev message metadata."""

    message_type = value if isinstance(value, type) else type(value)
    name = message_type.__dict__.get(EVENT_NAME_ATTRIBUTE)
    version = message_type.__dict__.get(EVENT_VERSION_ATTRIBUTE)
    return (
        isinstance(name, str)
        and bool(name)
        and isinstance(version, int)
        and not isinstance(version, bool)
        and version > 0
    )


def message_name(value: object) -> str:
    """Return the declared logical message name.

    Raises:
        MessageValidationError: If the message class has not been decorated.
    """

    message_type = value if isinstance(value, type) else type(value)
    name = message_type.__dict__.get(EVENT_NAME_ATTRIBUTE)
    if not isinstance(name, str) or not name:
        raise MessageValidationError(
            "Message type has no pyev event name; decorate it with @event",
            context={"message_class": _qualified_name(message_type)},
        )
    return name


def message_version(value: object) -> int:
    """Return the declared positive schema version for a message."""

    message_type = value if isinstance(value, type) else type(value)
    version = message_type.__dict__.get(EVENT_VERSION_ATTRIBUTE)
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise MessageValidationError(
            "Message type has no valid pyev schema version; decorate it with @event",
            context={"message_class": _qualified_name(message_type)},
        )
    return version


def message_to_payload(message: object) -> dict[str, JSONValue]:
    """Convert a supported typed message to a JSON-safe payload mapping.

    Dataclass instances, mappings, Pydantic-compatible objects, attrs
    instances, named tuples, and objects with a public ``to_dict`` method are
    supported.  Arbitrary object pickling is intentionally not attempted.
    """

    raw: object
    if isinstance(message, Mapping):
        raw = message
    elif is_dataclass(message) and not isinstance(message, type):
        raw = {field.name: getattr(message, field.name) for field in fields(message)}
    elif callable(model_dump := getattr(message, "model_dump", None)):
        try:
            raw = model_dump(mode="json")
        except Exception as exc:
            raise MessageValidationError(
                "Pydantic-compatible message could not be converted to a payload",
                context={"message_class": _qualified_name(type(message))},
            ) from exc
    elif hasattr(type(message), "__attrs_attrs__"):
        attributes = cast(Any, type(message)).__attrs_attrs__
        raw = {
            attribute.name: getattr(message, attribute.name)
            for attribute in attributes
            if isinstance(getattr(attribute, "name", None), str)
        }
    elif callable(as_dict := getattr(message, "_asdict", None)):
        raw = as_dict()
    elif callable(to_dict := getattr(message, "to_dict", None)):
        raw = to_dict()
    elif hasattr(message, "__dict__"):
        raw = {
            key: value
            for key, value in vars(message).items()
            if isinstance(key, str) and not key.startswith("_")
        }
    else:
        raise MessageValidationError(
            "Message must be a mapping or a supported typed model",
            context={"message_class": _qualified_name(type(message))},
        )

    normalized = to_json_value(raw)
    if not isinstance(normalized, dict):
        raise MessageValidationError("A message payload must be a JSON object")
    return normalized


def to_json_value(value: object, *, max_depth: int = 64) -> JSONValue:
    """Convert a value to the safe JSON data model used by envelopes.

    The conversion is deterministic and rejects binary data, non-string map
    keys, non-finite floats, sets, recursive structures, and unsupported
    objects.  ``datetime`` and ``UUID`` values are represented as strings.
    """

    if max_depth < 1:
        raise ValueError("max_depth must be positive")
    return _to_json_value(value, path="$", depth=0, max_depth=max_depth, seen=set())


def _to_json_value(
    value: object,
    *,
    path: str,
    depth: int,
    max_depth: int,
    seen: set[int],
) -> JSONValue:
    if depth > max_depth:
        raise MessageValidationError(
            "Message payload exceeds the maximum nesting depth",
            context={"path": path, "max_depth": max_depth},
        )
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise MessageValidationError(
                "Message payload contains a non-finite float", context={"path": path}
            )
        return value
    if isinstance(value, Enum):
        return _to_json_value(
            value.value, path=path, depth=depth + 1, max_depth=max_depth, seen=seen
        )
    if isinstance(value, datetime):
        return _datetime_to_text(value)
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal, Path, PurePath)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise MessageValidationError(
            "Binary values are not JSON-safe; use a binary serializer explicitly",
            context={"path": path},
        )

    if is_dataclass(value) and not isinstance(value, type):
        value = {field.name: getattr(value, field.name) for field in fields(value)}
    elif callable(model_dump := getattr(value, "model_dump", None)):
        try:
            value = model_dump(mode="json")
        except Exception as exc:
            raise MessageValidationError(
                "Model value could not be converted to JSON data",
                context={"path": path, "value_type": _qualified_name(type(value))},
            ) from exc

    if isinstance(value, Mapping):
        identity = id(value)
        if identity in seen:
            raise MessageValidationError(
                "Message payload contains a recursive mapping", context={"path": path}
            )
        seen.add(identity)
        try:
            result: dict[str, JSONValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise MessageValidationError(
                        "Message payload object keys must be strings",
                        context={"path": path, "key_type": type(key).__name__},
                    )
                result[key] = _to_json_value(
                    item,
                    path=f"{path}.{key}",
                    depth=depth + 1,
                    max_depth=max_depth,
                    seen=seen,
                )
            return result
        finally:
            seen.remove(identity)
    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in seen:
            raise MessageValidationError(
                "Message payload contains a recursive sequence", context={"path": path}
            )
        seen.add(identity)
        try:
            return [
                _to_json_value(
                    item,
                    path=f"{path}[{index}]",
                    depth=depth + 1,
                    max_depth=max_depth,
                    seen=seen,
                )
                for index, item in enumerate(value)
            ]
        finally:
            seen.remove(identity)
    if isinstance(value, (set, frozenset)):
        raise MessageValidationError(
            "Sets are not deterministic JSON values; use a sorted sequence",
            context={"path": path},
        )
    raise MessageValidationError(
        "Message payload contains an unsupported value",
        context={"path": path, "value_type": _qualified_name(type(value))},
    )


def _datetime_to_text(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(UTC)
        return value.isoformat().replace("+00:00", "Z")
    return value.isoformat()


def _qualified_name(value: type[object]) -> str:
    return f"{value.__module__}.{value.__qualname__}"


__all__ = [
    "EVENT_METADATA_ATTRIBUTE",
    "EVENT_NAME_ATTRIBUTE",
    "EVENT_VERSION_ATTRIBUTE",
    "Message",
    "MessageLike",
    "is_message",
    "message_name",
    "message_to_payload",
    "message_version",
    "to_json_value",
]
