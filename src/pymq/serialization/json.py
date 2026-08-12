"""Safe standard-library JSON serializer."""

from __future__ import annotations

import json
from typing import Any

from ..exceptions import MessageValidationError, SerializationError
from ..message import to_json_value
from .base import DeserializationContext, SerializationContext


class JsonSerializer:
    """Encode the pyev JSON data model as deterministic UTF-8 bytes."""

    name = "json"
    content_type = "application/json"

    def __init__(self, *, sort_keys: bool = True) -> None:
        self._sort_keys = sort_keys

    def encode(self, value: object, context: SerializationContext) -> bytes:
        """Normalize and encode a value as compact UTF-8 JSON."""

        del context
        try:
            normalized = to_json_value(value)
            return json.dumps(
                normalized,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=self._sort_keys,
            ).encode("utf-8")
        except MessageValidationError as exc:
            raise SerializationError("Value is not JSON serializable", context=exc.context) from exc
        except (TypeError, ValueError, UnicodeError) as exc:
            raise SerializationError("Value could not be encoded as UTF-8 JSON") from exc

    def decode(self, data: bytes, context: DeserializationContext) -> object:
        """Decode JSON while rejecting oversized data and duplicate keys."""

        if len(data) > context.max_size:
            raise SerializationError(
                "JSON payload exceeds the configured byte limit",
                context={"size": len(data), "max_size": context.max_size},
            )
        try:
            return json.loads(
                data.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise SerializationError("Payload is not valid UTF-8 JSON") from exc


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise ValueError(f"Non-finite JSON number is not permitted: {value}")


JSONSerializer = JsonSerializer


__all__ = ["JSONSerializer", "JsonSerializer"]
