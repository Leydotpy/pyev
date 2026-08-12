"""Explicitly unsafe Pickle serializer for trusted, internal data only."""

from __future__ import annotations

import pickle
import warnings

from ..exceptions import SerializationError, UnsafeSerializationError
from .base import DeserializationContext, SerializationContext


class UnsafeSerializerWarning(UserWarning):
    """Warning emitted when an unsafe serializer is explicitly enabled."""


class PickleSerializer:
    """Serialize trusted Python objects using Pickle.

    Pickle can execute arbitrary code while decoding.  Construction therefore
    requires ``allow_unsafe=True`` and decoding additionally requires a
    :class:`DeserializationContext` whose ``trusted`` flag is true.  This
    serializer is never registered by pyev's default registry.
    """

    name = "pickle"
    content_type = "application/x-python-pickle"

    def __init__(
        self,
        *,
        allow_unsafe: bool = False,
        protocol: int = pickle.HIGHEST_PROTOCOL,
    ) -> None:
        if not isinstance(protocol, int) or not 0 <= protocol <= pickle.HIGHEST_PROTOCOL:
            raise ValueError("protocol must be a supported pickle protocol number")
        self._allow_unsafe = allow_unsafe
        self._protocol = protocol
        if allow_unsafe:
            warnings.warn(
                "Pickle deserialization can execute arbitrary code; only use trusted data",
                UnsafeSerializerWarning,
                stacklevel=2,
            )

    def encode(self, value: object, context: SerializationContext) -> bytes:
        """Encode a value after checking explicit unsafe opt-in."""

        del context
        self._ensure_enabled()
        try:
            return pickle.dumps(value, protocol=self._protocol)
        except (pickle.PickleError, TypeError, AttributeError) as exc:
            raise SerializationError("Value could not be encoded with Pickle") from exc

    def decode(self, data: bytes, context: DeserializationContext) -> object:
        """Decode trusted Pickle bytes after both safety checks pass."""

        self._ensure_enabled()
        if not context.trusted:
            raise UnsafeSerializationError(
                "Pickle decoding requires DeserializationContext(trusted=True)",
                context={"serializer": self.name},
            )
        if len(data) > context.max_size:
            raise SerializationError(
                "Pickle payload exceeds the configured byte limit",
                context={"size": len(data), "max_size": context.max_size},
            )
        try:
            return pickle.loads(data)
        except (pickle.PickleError, EOFError, AttributeError, ImportError, IndexError) as exc:
            raise SerializationError("Payload is not valid Pickle data") from exc

    def _ensure_enabled(self) -> None:
        if not self._allow_unsafe:
            raise UnsafeSerializationError(
                "Pickle is disabled; construct PickleSerializer(allow_unsafe=True) explicitly",
                context={"serializer": self.name},
            )


__all__ = ["PickleSerializer", "UnsafeSerializerWarning"]
