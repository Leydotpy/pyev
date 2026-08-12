"""Lazy, deterministic Python entry-point discovery primitives."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from importlib import metadata

from broka.engines.base import BaseEngine
from broka.exceptions import PluginLoadError
from broka.registry import EngineRegistry

type EntryPointCollection = (
    metadata.EntryPoints
    | Mapping[str, Iterable[metadata.EntryPoint]]
    | Iterable[metadata.EntryPoint]
)
type EntryPointSource = Callable[[], EntryPointCollection]


@dataclass(frozen=True, slots=True)
class EntryPointDescriptor:
    """Stable metadata and an explicit loader for one installed entry point."""

    group: str
    name: str
    value: str
    distribution: str | None
    _entry_point: metadata.EntryPoint

    def load(self) -> object:
        """Load the target and translate failures to a safe typed error."""

        try:
            return self._entry_point.load()
        except Exception as error:
            raise PluginLoadError(
                f"failed to load plugin {self.name!r} from group {self.group!r}: "
                f"{type(error).__name__}",
                context={"plugin": self.name, "group": self.group, "value": self.value},
            ) from error


def _distribution_name(entry_point: metadata.EntryPoint) -> str | None:
    distribution = getattr(entry_point, "dist", None)
    if distribution is None:
        return None
    name = getattr(distribution, "name", None)
    return str(name) if name is not None else None


def iter_entry_points(
    group: str,
    *,
    source: EntryPointSource | None = None,
) -> tuple[EntryPointDescriptor, ...]:
    """Inspect one entry-point group without importing plugin targets."""

    if not group:
        raise ValueError("entry-point group must not be empty")
    discovered: EntryPointCollection
    if source is None:
        discovered = metadata.entry_points()
    else:
        discovered = source()
    selected: Iterable[metadata.EntryPoint]
    if isinstance(discovered, metadata.EntryPoints):
        selected = discovered.select(group=group)
    elif isinstance(discovered, Mapping):  # pragma: no cover - legacy provider compatibility
        selected = discovered.get(group, ())
    else:  # pragma: no cover - custom provider compatibility
        selected = (item for item in discovered if getattr(item, "group", None) == group)
    descriptors = (
        EntryPointDescriptor(
            group=group,
            name=entry_point.name,
            value=entry_point.value,
            distribution=_distribution_name(entry_point),
            _entry_point=entry_point,
        )
        for entry_point in selected
    )
    return tuple(
        sorted(
            descriptors,
            key=lambda item: (item.name, item.value, item.distribution or ""),
        )
    )


def load_entry_point(descriptor: EntryPointDescriptor) -> object:
    """Load a previously inspected descriptor."""

    return descriptor.load()


def discover_engines(
    registry: EngineRegistry,
    *,
    group: str = "pyev.engines",
    source: EntryPointSource | None = None,
    ignore_registered: bool = False,
) -> tuple[str, ...]:
    """Register installed engine entry points as lazy providers.

    Discovery reads package metadata only. Engine modules remain unloaded until
    the registry resolves or selects them.
    """

    registered: list[str] = []
    for descriptor in iter_entry_points(group, source=source):
        if descriptor.name in registry and ignore_registered:
            continue

        def loader(descriptor: EntryPointDescriptor = descriptor) -> type[BaseEngine]:
            value = descriptor.load()
            if not isinstance(value, type) or not issubclass(value, BaseEngine):
                raise PluginLoadError(
                    f"engine entry point {descriptor.name!r} did not provide a BaseEngine subclass",
                    context={"engine": descriptor.name, "value": descriptor.value},
                )
            return value

        registry.register_lazy(
            descriptor.name,
            loader,
            source=f"entry point {descriptor.value}",
        )
        registered.append(descriptor.name)
    return tuple(registered)


__all__ = [
    "EntryPointDescriptor",
    "EntryPointSource",
    "discover_engines",
    "iter_entry_points",
    "load_entry_point",
]
