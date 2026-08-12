"""Lazily imported optional MessagePack serializer."""

from __future__ import annotations

from importlib import import_module
from importlib.util import find_spec
from typing import Any

from ..exceptions import MessageValidationError, SerializationError
from ..message import to_json_value
from .base import DeserializationContext, SerializationContext


class MessagePackSerializer:
    """Encode JSON-safe values with the optional ``msgpack`` dependency.

    Importing :mod:`pyev` never imports MessagePack.  The dependency is loaded
    only when :meth:`encode` or :meth:`decode` is first called.
    """

    name = "msgpack"
    content_type = "application/msgpack"

    @classmethod
    def is_available(cls) -> bool:
        """Return whether the optional ``msgpack`` distribution is importable."""

        return find_spec("msgpack") is not None

    def encode(self, value: object, context: SerializationContext) -> bytes:
        """Normalize and encode a value as MessagePack bytes."""

        del context
        module = _load_msgpack()
        try:
            result = module.packb(to_json_value(value), use_bin_type=True)
        except MessageValidationError as exc:
            raise SerializationError(
                "Value is not safe for MessagePack serialization", context=exc.context
            ) from exc
        except Exception as exc:
            raise SerializationError("Value could not be encoded as MessagePack") from exc
        if not isinstance(result, bytes):
            raise SerializationError("MessagePack implementation returned non-byte output")
        return result

    def decode(self, data: bytes, context: DeserializationContext) -> object:
        """Decode MessagePack data and validate its JSON-safe data model."""

        if len(data) > context.max_size:
            raise SerializationError(
                "MessagePack payload exceeds the configured byte limit",
                context={"size": len(data), "max_size": context.max_size},
            )
        module = _load_msgpack()
        try:
            decoded = module.unpackb(data, raw=False, strict_map_key=True)
            return to_json_value(decoded)
        except MessageValidationError as exc:
            raise SerializationError(
                "Decoded MessagePack value is outside the safe pyev data model",
                context=exc.context,
            ) from exc
        except Exception as exc:
            raise SerializationError("Payload is not valid MessagePack") from exc


def _load_msgpack() -> Any:
    try:
        return import_module("msgpack")
    except ImportError as exc:
        raise SerializationError(
            "MessagePack support requires the optional 'msgpack' dependency",
            context={"dependency": "msgpack", "serializer": "msgpack"},
        ) from exc


MsgPackSerializer = MessagePackSerializer


__all__ = ["MessagePackSerializer", "MsgPackSerializer"]
