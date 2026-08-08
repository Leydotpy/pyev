from __future__ import annotations

from importlib.metadata import EntryPoint, EntryPoints

from pyev.discovery import discover_engines, iter_entry_points
from pyev.engines.local import LocalEngine
from pyev.plugins import PluginGroups, PluginRegistry, register_discovered_plugins
from pyev.registry import EngineRegistry


def _source() -> EntryPoints:
    return EntryPoints(
        (
            EntryPoint(
                name="local-plugin",
                value="pyev.engines.local:LocalEngine",
                group="pyev.engines",
            ),
            EntryPoint(name="text", value="builtins:str", group="pyev.serializers"),
        )
    )


def test_entry_point_inspection_is_grouped_and_deterministic() -> None:
    descriptors = iter_entry_points(PluginGroups.ENGINES, source=_source)

    assert [(item.name, item.value) for item in descriptors] == [
        ("local-plugin", "pyev.engines.local:LocalEngine")
    ]


def test_engine_discovery_registers_lazy_loader() -> None:
    registry = EngineRegistry()

    assert discover_engines(registry, source=_source) == ("local-plugin",)
    assert not registry.registrations[0].loaded
    assert registry.resolve("local-plugin") is LocalEngine


def test_generic_serializer_plugin_discovery_is_lazy() -> None:
    registry: PluginRegistry[object] = PluginRegistry()

    names = register_discovered_plugins(
        registry,
        PluginGroups.SERIALIZERS,
        source=_source,
    )

    assert names == ("text",)
    assert not registry.registrations[0].loaded
    assert registry.resolve("text") is str
