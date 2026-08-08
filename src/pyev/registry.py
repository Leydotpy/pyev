"""Isolated, typed engine registries with lazy loading and selection."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib import import_module
from importlib.util import find_spec
from typing import Final

from pyev.config import BrokerConfig
from pyev.engines.base import BaseEngine
from pyev.exceptions import (
    DuplicateRegistrationError,
    EngineUnavailableError,
    PluginLoadError,
    RegistryError,
)

type EngineLoader = Callable[[], type[BaseEngine]]


@dataclass(frozen=True, slots=True)
class EngineRegistration:
    """Read-only metadata describing a registered engine provider."""

    name: str
    priority: int
    source: str
    loaded: bool
    engine_type: type[BaseEngine] | None = None


@dataclass(slots=True)
class _EngineEntry:
    name: str
    loader: EngineLoader
    declared_priority: int
    source: str
    sequence: int
    engine_type: type[BaseEngine] | None = None

    def snapshot(self) -> EngineRegistration:
        priority = (
            int(getattr(self.engine_type, "priority", self.declared_priority))
            if self.engine_type is not None
            else self.declared_priority
        )
        return EngineRegistration(
            name=self.name,
            priority=priority,
            source=self.source,
            loaded=self.engine_type is not None,
            engine_type=self.engine_type,
        )


class EngineRegistry:
    """A broker-injectable registry of engine classes.

    Registry instances share no mutable state. Lazy loaders are invoked only by
    :meth:`resolve`, :meth:`create`, or automatic :meth:`select`.
    """

    def __init__(self) -> None:
        self._entries: dict[str, _EngineEntry] = {}
        self._sequence = 0

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def names(self) -> tuple[str, ...]:
        """Return registered names in deterministic registration order."""

        return tuple(
            entry.name for entry in sorted(self._entries.values(), key=lambda item: item.sequence)
        )

    @property
    def registrations(self) -> tuple[EngineRegistration, ...]:
        """Inspect registrations without triggering lazy loads."""

        return tuple(
            entry.snapshot()
            for entry in sorted(self._entries.values(), key=lambda item: item.sequence)
        )

    def register(
        self,
        engine_type: type[BaseEngine],
        *,
        name: str | None = None,
        source: str = "explicit",
    ) -> EngineRegistration:
        """Register an engine class explicitly."""

        if not isinstance(engine_type, type) or not issubclass(engine_type, BaseEngine):
            raise RegistryError("registered engines must subclass BaseEngine")
        engine_name = self._validate_name(name or engine_type.name)
        entry = self._add_entry(
            engine_name,
            loader=lambda: engine_type,
            priority=engine_type.priority,
            source=source,
        )
        entry.engine_type = engine_type
        return entry.snapshot()

    def register_lazy(
        self,
        name: str,
        loader: EngineLoader,
        *,
        priority: int = 0,
        source: str = "lazy",
    ) -> EngineRegistration:
        """Register an engine loader without importing its implementation."""

        if not callable(loader):
            raise RegistryError("engine loader must be callable")
        entry = self._add_entry(
            self._validate_name(name),
            loader=loader,
            priority=priority,
            source=source,
        )
        return entry.snapshot()

    def _add_entry(
        self,
        name: str,
        *,
        loader: EngineLoader,
        priority: int,
        source: str,
    ) -> _EngineEntry:
        if name in self._entries:
            existing = self._entries[name]
            raise DuplicateRegistrationError(
                f"engine {name!r} is already registered from {existing.source}",
                context={"engine": name, "existing_source": existing.source},
            )
        self._sequence += 1
        entry = _EngineEntry(name, loader, priority, source, self._sequence)
        self._entries[name] = entry
        return entry

    def unregister(self, name: str) -> EngineRegistration:
        """Remove a provider without forcing a lazy load."""

        try:
            return self._entries.pop(name).snapshot()
        except KeyError as error:
            raise RegistryError(f"unknown engine {name!r}", context={"engine": name}) from error

    def resolve(self, name: str) -> type[BaseEngine]:
        """Resolve and validate one engine class, invoking its loader once."""

        try:
            entry = self._entries[name]
        except KeyError as error:
            available = ", ".join(self.names) or "none"
            raise RegistryError(
                f"unknown engine {name!r}; registered engines: {available}",
                context={"engine": name},
            ) from error
        if entry.engine_type is not None:
            return entry.engine_type
        try:
            loaded = entry.loader()
        except Exception as error:
            raise PluginLoadError(
                f"failed to load engine plugin {name!r} from {entry.source}: "
                f"{type(error).__name__}",
                context={"engine": name, "source": entry.source},
            ) from error
        if not isinstance(loaded, type) or not issubclass(loaded, BaseEngine):
            raise PluginLoadError(
                f"engine plugin {name!r} did not provide a BaseEngine subclass",
                context={"engine": name, "source": entry.source},
            )
        entry.engine_type = loaded
        return loaded

    get = resolve

    def create(
        self,
        name: str,
        config: BrokerConfig | Mapping[str, object] | None = None,
    ) -> BaseEngine:
        """Instantiate one engine with only its own revealed settings."""

        broker_config = (
            config if isinstance(config, BrokerConfig) else BrokerConfig.from_mapping(config)
        )
        engine_type = self._require_available(name, broker_config)
        settings = broker_config.engine_settings(name).as_dict(reveal_secrets=True)
        return engine_type(settings)

    def select(
        self,
        config: BrokerConfig | Mapping[str, object] | None = None,
        *,
        requested: str | None = None,
    ) -> type[BaseEngine]:
        """Select an available engine according to the specification order."""

        broker_config = (
            config if isinstance(config, BrokerConfig) else BrokerConfig.from_mapping(config)
        )
        selected_name = requested or broker_config.engine
        if selected_name is not None:
            return self._require_available(selected_name, broker_config)

        candidates: list[tuple[int, int, str, type[BaseEngine]]] = []
        failures: dict[str, str] = {}
        for entry in self._entries.values():
            if entry.name == "memory" and not broker_config.allow_memory_fallback:
                continue
            try:
                engine_type = self.resolve(entry.name)
                availability = engine_type.is_available(broker_config.engine_settings(entry.name))
            except Exception as error:
                failures[entry.name] = type(error).__name__
                continue
            if bool(availability):
                candidates.append(
                    (
                        -int(getattr(engine_type, "priority", entry.declared_priority)),
                        entry.sequence,
                        entry.name,
                        engine_type,
                    )
                )
            else:
                failures[entry.name] = getattr(availability, "reason", None) or "unavailable"
        if not candidates:
            raise EngineUnavailableError(
                "no registered transport engine is available; configure an engine explicitly "
                "or permit the memory fallback",
                context={"engines": tuple(self.names), "failures": failures},
            )
        candidates.sort()
        return candidates[0][3]

    def _require_available(
        self,
        name: str,
        config: BrokerConfig,
    ) -> type[BaseEngine]:
        engine_type = self.resolve(name)
        try:
            availability = engine_type.is_available(config.engine_settings(name))
        except Exception as error:
            raise EngineUnavailableError(
                f"engine {name!r} availability check failed with {type(error).__name__}",
                context={"engine": name},
            ) from error
        if not bool(availability):
            reason = getattr(availability, "reason", None) or "requirements are not satisfied"
            raise EngineUnavailableError(
                f"engine {name!r} is unavailable: {reason}",
                context={"engine": name},
            )
        return engine_type

    @staticmethod
    def _validate_name(name: str) -> str:
        name = name.strip().casefold()
        if not name or any(character.isspace() for character in name):
            raise RegistryError("engine names must be non-empty and contain no whitespace")
        return name


_OPTIONAL_ENGINES: Final[tuple[tuple[str, str, str, int], ...]] = (
    ("redis", "pyev.engines.redis", "RedisEngine", 50),
    ("rabbitmq", "pyev.engines.rabbitmq", "RabbitMQEngine", 40),
    ("kafka", "pyev.engines.kafka", "KafkaEngine", 30),
)


def _import_engine(module_name: str, attribute: str) -> type[BaseEngine]:
    value = getattr(import_module(module_name), attribute)
    if not isinstance(value, type) or not issubclass(value, BaseEngine):
        raise PluginLoadError(
            f"{module_name}:{attribute} did not provide a BaseEngine subclass",
            context={"module": module_name, "attribute": attribute},
        )
    return value


def _lazy_import_engine(module_name: str, attribute: str) -> EngineLoader:
    def load() -> type[BaseEngine]:
        return _import_engine(module_name, attribute)

    return load


def create_default_registry() -> EngineRegistry:
    """Create a fresh registry containing built-ins and installed official plugins."""

    from pyev.engines.local import LocalEngine
    from pyev.engines.memory import MemoryEngine

    registry = EngineRegistry()
    registry.register(LocalEngine, source="built-in")
    registry.register(MemoryEngine, source="built-in")
    for name, module_name, attribute, priority in _OPTIONAL_ENGINES:
        if find_spec(module_name) is not None:
            registry.register_lazy(
                name,
                _lazy_import_engine(module_name, attribute),
                priority=priority,
                source=f"optional package {module_name}",
            )
    return registry


# A function alias deliberately avoids a process-wide mutable registry singleton.
default_engine_registry = create_default_registry


__all__ = [
    "EngineLoader",
    "EngineRegistration",
    "EngineRegistry",
    "create_default_registry",
    "default_engine_registry",
]
