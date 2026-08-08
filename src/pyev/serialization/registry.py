"""Isolated serializer registration and content-type resolution."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from threading import RLock
from types import MappingProxyType

from ..exceptions import DuplicateRegistrationError, SerializationError
from .base import DeserializationContext, SerializationContext, Serializer
from .json import JsonSerializer
from .msgpack import MessagePackSerializer


class SerializerRegistry:
    """A thread-safe registry of named serializer instances."""

    def __init__(self, *, default: str | None = None) -> None:
        self._serializers: dict[str, Serializer] = {}
        self._content_types: dict[str, str] = {}
        self._default = default
        self._lock = RLock()

    @classmethod
    def with_defaults(cls) -> SerializerRegistry:
        """Create a registry with JSON default and lazy MessagePack support."""

        registry = cls(default="json")
        registry.register(JsonSerializer())
        registry.register(MessagePackSerializer())
        return registry

    @property
    def default_name(self) -> str | None:
        """Return the configured default serializer name."""

        return self._default

    @property
    def default(self) -> Serializer:
        """Return the default serializer or raise when none is configured."""

        if self._default is None:
            raise SerializationError("No default serializer is configured")
        return self.get(self._default)

    def register(
        self,
        serializer: Serializer,
        *,
        replace: bool = False,
        make_default: bool = False,
    ) -> Serializer:
        """Register and return a serializer after validating its contract."""

        name = getattr(serializer, "name", None)
        content_type = getattr(serializer, "content_type", None)
        if not isinstance(name, str) or not name.strip():
            raise SerializationError("Serializer name must be a non-empty string")
        if not isinstance(content_type, str) or not content_type.strip():
            raise SerializationError("Serializer content_type must be a non-empty string")
        if not callable(getattr(serializer, "encode", None)) or not callable(
            getattr(serializer, "decode", None)
        ):
            raise SerializationError("Serializer must define callable encode and decode methods")
        normalized_name = name.strip().lower()
        normalized_content_type = _normalize_content_type(content_type)
        if not normalized_content_type:
            raise SerializationError("Serializer content_type must contain a MIME type")
        with self._lock:
            existing = self._serializers.get(normalized_name)
            content_owner = self._content_types.get(normalized_content_type)
            if existing is not None and existing is not serializer and not replace:
                raise DuplicateRegistrationError(
                    f"Serializer {normalized_name!r} is already registered",
                    context={"serializer": normalized_name},
                )
            if content_owner is not None and content_owner != normalized_name and not replace:
                raise DuplicateRegistrationError(
                    f"Content type {normalized_content_type!r} is already registered",
                    context={
                        "content_type": normalized_content_type,
                        "serializer": content_owner,
                    },
                )
            if existing is not None:
                old_content_type = _normalize_content_type(existing.content_type)
                self._content_types.pop(old_content_type, None)
            if content_owner is not None and content_owner != normalized_name:
                self._serializers.pop(content_owner, None)
                if self._default == content_owner:
                    self._default = normalized_name
            self._serializers[normalized_name] = serializer
            self._content_types[normalized_content_type] = normalized_name
            if make_default or self._default is None:
                self._default = normalized_name
        return serializer

    def unregister(self, name: str) -> Serializer | None:
        """Remove and return a serializer without affecting other registries."""

        normalized = name.strip().lower()
        with self._lock:
            serializer = self._serializers.pop(normalized, None)
            if serializer is not None:
                self._content_types.pop(_normalize_content_type(serializer.content_type), None)
                if self._default == normalized:
                    self._default = None
            return serializer

    def get(self, name: str) -> Serializer:
        """Resolve a serializer by its case-insensitive registered name."""

        normalized = name.strip().lower()
        with self._lock:
            try:
                return self._serializers[normalized]
            except KeyError as exc:
                raise SerializationError(
                    f"Unknown serializer {name!r}", context={"serializer": name}
                ) from exc

    def for_content_type(self, content_type: str) -> Serializer:
        """Resolve a serializer by MIME content type, ignoring parameters."""

        normalized = _normalize_content_type(content_type)
        with self._lock:
            try:
                name = self._content_types[normalized]
                return self._serializers[name]
            except KeyError as exc:
                raise SerializationError(
                    f"Unsupported content type {content_type!r}",
                    context={"content_type": content_type},
                ) from exc

    def resolve(
        self,
        *,
        name: str | None = None,
        content_type: str | None = None,
    ) -> Serializer:
        """Resolve by name, content type, or the configured default.

        When both selectors are supplied, they must identify the same
        serializer so envelope metadata cannot be interpreted ambiguously.
        """

        if name is None and content_type is None:
            return self.default
        if name is None:
            assert content_type is not None
            return self.for_content_type(content_type)
        serializer = self.get(name)
        if content_type is not None and _normalize_content_type(
            serializer.content_type
        ) != _normalize_content_type(content_type):
            raise SerializationError(
                "Serializer name and content type do not match",
                context={"serializer": name, "content_type": content_type},
            )
        return serializer

    def encode(
        self,
        value: object,
        *,
        name: str | None = None,
        context: SerializationContext | None = None,
    ) -> bytes:
        """Resolve a serializer and encode one value."""

        return self.resolve(name=name).encode(value, context or SerializationContext())

    def decode(
        self,
        data: bytes,
        *,
        name: str | None = None,
        content_type: str | None = None,
        context: DeserializationContext | None = None,
    ) -> object:
        """Resolve a serializer and decode one byte payload."""

        return self.resolve(name=name, content_type=content_type).decode(
            data, context or DeserializationContext()
        )

    def snapshot(self) -> Mapping[str, Serializer]:
        """Return an immutable snapshot of registered serializers."""

        with self._lock:
            return MappingProxyType(dict(self._serializers))

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        with self._lock:
            return name.strip().lower() in self._serializers

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(tuple(self._serializers))

    def __len__(self) -> int:
        with self._lock:
            return len(self._serializers)


def _normalize_content_type(value: str) -> str:
    return value.split(";", 1)[0].strip().lower()


default_serializer_registry = SerializerRegistry.with_defaults()
DEFAULT_SERIALIZER_REGISTRY = default_serializer_registry


__all__ = [
    "DEFAULT_SERIALIZER_REGISTRY",
    "SerializerRegistry",
    "default_serializer_registry",
]
