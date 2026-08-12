"""Serializer contracts and immutable operation contexts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class SerializationContext:
    """Context supplied to a serializer for one encode operation."""

    message_type: str | None = None
    schema_version: int | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class DeserializationContext:
    """Context supplied to a serializer for one decode operation.

    ``trusted`` must only be set when bytes come from an authenticated and
    fully trusted source.  Unsafe serializers such as Pickle inspect it.
    """

    message_type: str | None = None
    schema_version: int | None = None
    trusted: bool = False
    max_size: int = 16 * 1024 * 1024
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.max_size < 1:
            raise ValueError("max_size must be positive")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@runtime_checkable
class Serializer(Protocol):
    """Structural interface implemented by pyev serializers."""

    name: str
    content_type: str

    def encode(self, value: object, context: SerializationContext) -> bytes:
        """Encode ``value`` to transport bytes."""

    def decode(self, data: bytes, context: DeserializationContext) -> object:
        """Decode transport bytes to a safe Python representation."""


__all__ = ["DeserializationContext", "SerializationContext", "Serializer"]
