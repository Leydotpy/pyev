"""Generic plugin registries and pyev entry-point group helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import cast

from pyev.discovery import EntryPointDescriptor, EntryPointSource, iter_entry_points
from pyev.exceptions import DuplicateRegistrationError, PluginLoadError, RegistryError


class PluginGroups:
    """Canonical entry-point group names for public extension surfaces."""

    ENGINES = "pyev.engines"
    SERIALIZERS = "pyev.serializers"
    MIDDLEWARE = "pyev.middleware"
    ROUTERS = "pyev.routers"
    DEADLETTER_STORES = "pyev.deadletter_stores"
    CONFIG_PROVIDERS = "pyev.config_providers"
    METRICS_EXPORTERS = "pyev.metrics_exporters"
    TRACING_PROVIDERS = "pyev.tracing_providers"


@dataclass(slots=True)
class _PluginEntry[PluginT]:
    loader: Callable[[], PluginT]
    source: str
    value: PluginT | None = None
    loaded: bool = False


@dataclass(frozen=True, slots=True)
class PluginRegistration[PluginT]:
    """An introspection-safe generic plugin registration."""

    name: str
    source: str
    loaded: bool
    value: PluginT | None = None


class PluginRegistry[PluginT]:
    """An isolated named registry suitable for serializers and middleware."""

    def __init__(self) -> None:
        self._entries: dict[str, _PluginEntry[PluginT]] = {}

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[str]:
        return iter(self._entries)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._entries)

    @property
    def registrations(self) -> tuple[PluginRegistration[PluginT], ...]:
        return tuple(
            PluginRegistration(name, entry.source, entry.loaded, entry.value)
            for name, entry in self._entries.items()
        )

    def register(self, name: str, value: PluginT, *, source: str = "explicit") -> None:
        self._add(name, _PluginEntry(lambda: value, source, value, True))

    def register_lazy(
        self,
        name: str,
        loader: Callable[[], PluginT],
        *,
        source: str = "lazy",
    ) -> None:
        if not callable(loader):
            raise RegistryError("plugin loader must be callable")
        self._add(name, _PluginEntry(loader, source))

    def _add(self, name: str, entry: _PluginEntry[PluginT]) -> None:
        name = name.strip()
        if not name:
            raise RegistryError("plugin names must not be empty")
        if name in self._entries:
            raise DuplicateRegistrationError(
                f"plugin {name!r} is already registered",
                context={"plugin": name},
            )
        self._entries[name] = entry

    def resolve(self, name: str) -> PluginT:
        try:
            entry = self._entries[name]
        except KeyError as error:
            raise RegistryError(f"unknown plugin {name!r}", context={"plugin": name}) from error
        if entry.loaded:
            return cast(PluginT, entry.value)
        try:
            value = entry.loader()
        except PluginLoadError:
            raise
        except Exception as error:
            raise PluginLoadError(
                f"failed to load plugin {name!r} from {entry.source}: {type(error).__name__}",
                context={"plugin": name, "source": entry.source},
            ) from error
        entry.value = value
        entry.loaded = True
        return value

    get = resolve

    def unregister(self, name: str) -> PluginRegistration[PluginT]:
        try:
            entry = self._entries.pop(name)
        except KeyError as error:
            raise RegistryError(f"unknown plugin {name!r}", context={"plugin": name}) from error
        return PluginRegistration(name, entry.source, entry.loaded, entry.value)


def discover_plugins(
    group: str,
    *,
    source: EntryPointSource | None = None,
) -> tuple[EntryPointDescriptor, ...]:
    """Inspect generic plugins without importing their targets."""

    return iter_entry_points(group, source=source)


def register_discovered_plugins(
    registry: PluginRegistry[object],
    group: str,
    *,
    source: EntryPointSource | None = None,
    ignore_registered: bool = False,
) -> tuple[str, ...]:
    """Register all targets in a group as lazy generic plugins."""

    names: list[str] = []
    for descriptor in discover_plugins(group, source=source):
        if descriptor.name in registry and ignore_registered:
            continue
        registry.register_lazy(
            descriptor.name,
            descriptor.load,
            source=f"entry point {descriptor.value}",
        )
        names.append(descriptor.name)
    return tuple(names)


def discover_serializer_plugins(
    registry: PluginRegistry[object] | None = None,
    *,
    source: EntryPointSource | None = None,
) -> PluginRegistry[object]:
    """Populate and return a serializer plugin registry lazily."""

    target = registry if registry is not None else PluginRegistry[object]()
    register_discovered_plugins(target, PluginGroups.SERIALIZERS, source=source)
    return target


__all__ = [
    "PluginGroups",
    "PluginRegistration",
    "PluginRegistry",
    "discover_plugins",
    "discover_serializer_plugins",
    "register_discovered_plugins",
]
