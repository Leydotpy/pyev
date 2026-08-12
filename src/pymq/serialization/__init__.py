"""Serialization protocols, registries, and bundled serializers."""

from .base import DeserializationContext, SerializationContext, Serializer
from .json import JSONSerializer, JsonSerializer
from .msgpack import MessagePackSerializer, MsgPackSerializer
from .pickle import PickleSerializer, UnsafeSerializerWarning
from .registry import (
    DEFAULT_SERIALIZER_REGISTRY,
    SerializerRegistry,
    default_serializer_registry,
)

__all__ = [
    "DEFAULT_SERIALIZER_REGISTRY",
    "DeserializationContext",
    "JSONSerializer",
    "JsonSerializer",
    "MessagePackSerializer",
    "MsgPackSerializer",
    "PickleSerializer",
    "SerializationContext",
    "Serializer",
    "SerializerRegistry",
    "UnsafeSerializerWarning",
    "default_serializer_registry",
]
