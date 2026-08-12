from __future__ import annotations

import logging
from collections.abc import Mapping
from importlib.metadata import EntryPoint, EntryPoints
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar

import pytest

import pymq.config as config_module
from pymq.broker import Broker, BrokerState
from pymq.config import (
    BrokerConfig,
    ConfigLoader,
    FrozenMapping,
    SecretValue,
    SerializationSettings,
    deep_merge,
    environment_config,
    load_config,
    read_config_file,
    redact,
    reveal_secret,
)
from pymq.exceptions import (
    ConfigurationError,
    DuplicateRegistrationError,
    PluginLoadError,
    RegistryError,
    SerializationError,
)
from pymq.factory import BrokerFactory
from pymq.observability.logging import StructuredLogAdapter, get_logger
from pymq.observability.redaction import (
    REDACTED,
    is_sensitive_key,
    redact_mapping,
    redact_text,
    redact_value,
)
from pymq.plugins import (
    PluginGroups,
    PluginRegistry,
    discover_plugins,
    discover_serializer_plugins,
    register_discovered_plugins,
)
from pymq.routing import Router
from pymq.serialization import DeserializationContext, SerializationContext
from pymq.serialization.registry import SerializerRegistry


class _Serializer:
    def __init__(self, name: str, content_type: str) -> None:
        self.name = name
        self.content_type = content_type
        self.encoded_context: SerializationContext | None = None
        self.decoded_context: DeserializationContext | None = None

    def encode(self, value: object, context: SerializationContext) -> bytes:
        self.encoded_context = context
        return f"encoded:{value}".encode()

    def decode(self, data: bytes, context: DeserializationContext) -> object:
        self.decoded_context = context
        return data.decode()


class _ModelProvider:
    def model_dump(self) -> Mapping[str, object]:
        return {"engine": "local", "source": "model"}


class _LoadProvider:
    def load(self) -> Mapping[str, object]:
        return {"source": "provider"}


class _SettingsProvider:
    PYEV: ClassVar[dict[str, object]] = {"source": "settings"}


class _LegacySettingsProvider:
    PYEV_CONFIG: ClassVar[dict[str, object]] = {"source": "legacy-settings"}


def _plugin_source(*entries: EntryPoint) -> EntryPoints:
    return EntryPoints(entries)


def test_secret_and_frozen_collection_helpers_cover_all_container_shapes() -> None:
    secret = SecretValue("open-sesame")
    value = FrozenMapping(
        {
            "token": secret,
            "nested": [{"api-key": "hidden"}, {"plain"}],
            "empty_secret": "",
        }
    )

    assert secret.reveal() == "open-sesame"
    assert repr(secret) == "SecretValue('********')"
    assert str(secret) == "********"
    assert secret
    assert not SecretValue("")
    assert len(value) == 3
    assert set(iter(value)) == {"token", "nested", "empty_secret"}
    assert "open-sesame" not in repr(value)
    assert value.get_path(("nested", "missing"), "fallback") == "fallback"
    assert value.get_path("nested.0", "not-a-mapping") == "not-a-mapping"

    revealed = reveal_secret(value)
    assert revealed["token"] == "open-sesame"  # type: ignore[index]
    assert isinstance(revealed["nested"], list)  # type: ignore[index]
    assert redact(secret) == "********"
    assert redact("value", key="access.token") == "********"
    assert redact({"ordinary": {1, 2}}) == {"ordinary": [1, 2]}
    assert redact(42) == 42


def test_url_redaction_handles_ipv6_ports_plain_text_and_malformed_ports() -> None:
    assert redact("not-a-url") == "not-a-url"
    assert redact("redis://user:password@example.test/0") == "redis://********@example.test/0"
    assert (
        redact("redis://user:password@[2001:db8::1]:6379/0?ssl=true")
        == "redis://********@[2001:db8::1]:6379/0?ssl=true"
    )
    assert redact("redis://user:password@example.test:bad/0") == (
        "redis://********@example.test:bad/0"
    )
    assert redact("redis://example.test:bad/0") == "redis://example.test:bad/0"
    assert config_module._redact_url("http://[broken") == "http://[broken"


@pytest.mark.parametrize("value", [12, [], object()])
def test_serialization_settings_reject_invalid_top_level_types(value: object) -> None:
    with pytest.raises(ConfigurationError, match="serialization"):
        SerializationSettings.from_value(value)


def test_serialization_settings_support_shorthand_options_and_identity() -> None:
    shorthand = SerializationSettings.from_value("custom")
    configured = SerializationSettings.from_value(
        {
            "default": "json",
            "allowed": "json",
            "indent": 2,
            "api_token": "sensitive",
        }
    )

    assert SerializationSettings.from_value(shorthand) is shorthand
    assert shorthand.allowed == ("custom",)
    assert configured.allowed == ("json",)
    assert configured.options["indent"] == 2
    assert configured.as_dict()["api_token"] == "********"
    assert configured.as_dict(reveal_secrets=True)["api_token"] == "sensitive"


@pytest.mark.parametrize(
    ("value", "path"),
    [
        ({"default": ""}, "serialization.default"),
        ({"default": 3}, "serialization.default"),
        ({"default": "json", "allowed": 3}, "serialization.allowed"),
        ({"default": "json", "allowed": ["msgpack"]}, "serialization.allowed"),
    ],
)
def test_serialization_settings_validation_is_actionable(
    value: Mapping[str, object], path: str
) -> None:
    with pytest.raises(ConfigurationError, match=path):
        SerializationSettings.from_value(value)


@pytest.mark.parametrize(
    ("values", "path"),
    [
        ({"engine": ""}, "engine"),
        ({"engine": 1}, "engine"),
        ({"source": 1}, "source"),
        ({"allow_memory_fallback": "yes"}, "allow_memory_fallback"),
        ({"reliability": 1}, "reliability"),
    ],
)
def test_broker_config_rejects_invalid_sections(values: dict[str, object], path: str) -> None:
    with pytest.raises(ConfigurationError, match=path):
        BrokerConfig.from_mapping(values)


def test_broker_config_identity_extra_sections_and_overrides() -> None:
    config = BrokerConfig.from_mapping(
        {
            "default_engine": "local",
            "source": "before",
            "engines": {"local": None},
            "reliability": None,
            "middleware": None,
            "routing": None,
            "lifecycle": None,
            "custom": {"enabled": True},
        }
    )

    assert BrokerConfig.from_mapping(config) is config
    assert config.default_engine == "local"
    assert len(config.engine_settings()) == 0
    assert config.extra.get_path("custom.enabled") is True

    overridden = config.with_overrides(
        {"source": "after", "custom": {"mode": "strict"}, "engine": None}
    )
    assert overridden.source == "after"
    assert overridden.engine is None
    assert len(overridden.engine_settings()) == 0
    assert overridden.extra.get_path("custom.enabled") is True
    assert overridden.extra.get_path("custom.mode") == "strict"

    malformed = BrokerConfig(engine="local", engines=FrozenMapping({"local": 3}))
    with pytest.raises(ConfigurationError, match=r"engines\.local"):
        malformed.engine_settings()


def test_config_loader_supports_model_provider_load_provider_and_settings_objects() -> None:
    loaded = ConfigLoader().load(
        defaults=BrokerConfig.from_mapping({"source": "default", "engine": "memory"}),
        framework=_ModelProvider(),
        overrides=_LoadProvider(),
        include_environment=False,
    )
    settings = ConfigLoader().load(framework=_SettingsProvider(), include_environment=False)
    legacy = ConfigLoader().load(framework=_LegacySettingsProvider(), include_environment=False)

    assert loaded.engine == "local"
    assert loaded.source == "provider"
    assert settings.source == "settings"
    assert legacy.source == "legacy-settings"


@pytest.mark.parametrize(
    "provider",
    [
        object(),
        type("BadModel", (), {"model_dump": lambda self: "invalid"})(),
        type("BadLoader", (), {"load": lambda self: 42})(),
        type("BadSettings", (), {"PYEV": "invalid"})(),
    ],
)
def test_config_loader_rejects_unsupported_or_invalid_providers(provider: object) -> None:
    with pytest.raises(ConfigurationError):
        ConfigLoader().load(defaults=provider, include_environment=False)


def test_deep_merge_replaces_scalars_and_recursively_merges_mappings() -> None:
    assert deep_merge(
        {"engine": "memory", "nested": {"left": 1}, "replace": {"old": True}},
        {"nested": {"right": 2}, "replace": "new", 3: "coerced"},  # type: ignore[dict-item]
    ) == {
        "engine": "memory",
        "nested": {"left": 1, "right": 2},
        "replace": "new",
        "3": "coerced",
    }


def test_read_config_file_supports_json_toml_and_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    json_path = tmp_path / "config.json"
    toml_path = tmp_path / "config.toml"
    yaml_path = tmp_path / "config.yaml"
    json_path.write_text('{"engine":"memory"}', encoding="utf-8")
    toml_path.write_text("[engines.memory]\ncapacity = 8\n", encoding="utf-8")
    yaml_path.write_text("engine: local\n", encoding="utf-8")

    class _Yaml:
        @staticmethod
        def safe_load(content: bytes) -> object:
            assert content.replace(b"\r\n", b"\n") == b"engine: local\n"
            return {"engine": "local"}

    original_import = config_module.import_module

    def import_module(name: str) -> Any:
        return _Yaml if name == "yaml" else original_import(name)

    monkeypatch.setattr(config_module, "import_module", import_module)

    assert read_config_file(json_path) == {"engine": "memory"}
    assert read_config_file(toml_path) == {"engines": {"memory": {"capacity": 8}}}
    assert read_config_file(yaml_path) == {"engine": "local"}


def test_read_config_file_reports_missing_yaml_invalid_and_unsupported_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    yaml_path = tmp_path / "config.yml"
    yaml_path.write_text("engine: local", encoding="utf-8")

    def missing_yaml(name: str) -> Any:
        assert name == "yaml"
        raise ImportError(name)

    monkeypatch.setattr(config_module, "import_module", missing_yaml)
    with pytest.raises(ConfigurationError, match="PyYAML"):
        read_config_file(yaml_path)

    invalid_json = tmp_path / "invalid.json"
    invalid_json.write_text("{", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="invalid json configuration"):
        read_config_file(invalid_json)

    invalid_toml = tmp_path / "invalid.toml"
    invalid_toml.write_text("[broken", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="invalid toml configuration"):
        read_config_file(invalid_toml)

    scalar_json = tmp_path / "scalar.json"
    scalar_json.write_text("42", encoding="utf-8")
    with pytest.raises(ConfigurationError, match=r"scalar\.json"):
        read_config_file(scalar_json)

    unsupported = tmp_path / "config.ini"
    unsupported.write_text("engine=memory", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="unsupported configuration file type"):
        read_config_file(unsupported)

    with pytest.raises(ConfigurationError, match="unable to read"):
        read_config_file(tmp_path / "missing.json")


def test_environment_config_handles_empty_raw_nested_and_collision_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = environment_config(
        {
            "PYEV_": "ignored",
            "PYEV___": "ignored",
            "PYEV_EMPTY": "   ",
            "PYEV_RAW": "not-json",
            "PYEV_NODE": "leaf",
            "PYEV_NODE__CHILD": '"nested"',
            "PYEV_TREE__LEFT": "1",
            "PYEV_TREE__RIGHT": "2",
            "PYEV_LIST": "[1, 2]",
        }
    )

    assert values == {
        "empty": "",
        "list": [1, 2],
        "node": {"child": "nested"},
        "raw": "not-json",
        "tree": {"left": 1, "right": 2},
    }

    monkeypatch.setenv("CUSTOM_ENGINE", '"local"')
    assert environment_config(prefix="CUSTOM_")["engine"] == "local"


def test_config_loader_files_wrappers_and_environment_controls(tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.toml"
    first.write_text('{"engine":"local","source":"first"}', encoding="utf-8")
    second.write_text('source = "second"\n', encoding="utf-8")

    with pytest.raises(ConfigurationError, match="prefix"):
        ConfigLoader(env_prefix="")

    loaded = BrokerConfig.load(
        files=(first, second),
        environ={"PYEV_ENGINE": '"ignored"'},
        include_environment=False,
    )
    convenience = load_config(files=str(first), include_environment=False)

    assert loaded.engine == "local"
    assert loaded.source == "second"
    assert convenience.source == "first"


@pytest.mark.parametrize(
    "serializer",
    [
        object(),
        type("MissingName", (), {"name": "", "content_type": "application/x"})(),
        type("MissingType", (), {"name": "x", "content_type": ""})(),
        type("EmptyMime", (), {"name": "x", "content_type": "; charset=utf-8"})(),
        type("NoEncode", (), {"name": "x", "content_type": "application/x"})(),
    ],
)
def test_serializer_registry_rejects_invalid_contracts(serializer: object) -> None:
    with pytest.raises(SerializationError):
        SerializerRegistry().register(serializer)  # type: ignore[arg-type]


def test_serializer_registry_rejects_parameter_only_content_type() -> None:
    with pytest.raises(SerializationError, match="MIME type"):
        SerializerRegistry().register(_Serializer("custom", "; charset=utf-8"))


def test_serializer_registry_duplicate_name_and_content_type_are_explicit() -> None:
    registry = SerializerRegistry()
    first = _Serializer("first", "application/x-first")
    registry.register(first)

    with pytest.raises(DuplicateRegistrationError, match="first"):
        registry.register(_Serializer(" FIRST ", "application/x-other"))
    with pytest.raises(DuplicateRegistrationError, match="Content type"):
        registry.register(_Serializer("second", "Application/X-First; charset=utf-8"))

    assert registry.register(first) is first


def test_serializer_registry_replace_updates_owners_defaults_and_content_types() -> None:
    registry = SerializerRegistry()
    first = _Serializer("first", "application/x-first")
    second = _Serializer("second", "application/x-second")
    registry.register(first)
    registry.register(second, make_default=True)

    renamed = _Serializer("third", "application/x-second")
    registry.register(renamed, replace=True)
    assert registry.default_name == "third"
    assert "second" not in registry
    assert registry.for_content_type("application/x-second") is renamed

    changed = _Serializer("third", "application/x-third")
    registry.register(changed, replace=True)
    assert registry.for_content_type("application/x-third") is changed
    with pytest.raises(SerializationError, match="Unsupported content type"):
        registry.for_content_type("application/x-second")


def test_serializer_registry_unregister_resolution_and_collection_views() -> None:
    registry = SerializerRegistry()
    serializer = _Serializer(" custom ", "Application/X-Custom; version=1")
    registry.register(serializer)

    assert registry.default_name == "custom"
    assert registry.resolve() is serializer
    assert registry.get("CUSTOM") is serializer
    assert registry.resolve(content_type="application/x-custom; charset=utf-8") is serializer
    assert len(registry) == 1
    assert tuple(registry) == ("custom",)
    assert 1 not in registry
    snapshot = registry.snapshot()
    assert isinstance(snapshot, MappingProxyType)
    with pytest.raises(TypeError):
        snapshot["other"] = serializer  # type: ignore[index]

    assert registry.unregister("missing") is None
    assert registry.unregister(" CUSTOM ") is serializer
    assert registry.default_name is None
    with pytest.raises(SerializationError, match="No default"):
        _ = registry.default
    with pytest.raises(SerializationError, match="Unknown serializer"):
        registry.get("missing")

    first = _Serializer("first", "application/x-first")
    second = _Serializer("second", "application/x-second")
    registry.register(first)
    registry.register(second, make_default=True)
    assert registry.unregister("first") is first
    assert registry.default is second


def test_serializer_registry_encode_and_decode_forward_default_and_explicit_contexts() -> None:
    registry = SerializerRegistry()
    serializer = _Serializer("custom", "application/x-custom")
    registry.register(serializer)

    assert registry.encode({"value": 1}) == b"encoded:{'value': 1}"
    assert isinstance(serializer.encoded_context, SerializationContext)

    serialization_context = SerializationContext(message_type="tests.message")
    registry.encode("value", name="custom", context=serialization_context)
    assert serializer.encoded_context is serialization_context

    assert registry.decode(b"decoded", content_type="application/x-custom") == "decoded"
    assert isinstance(serializer.decoded_context, DeserializationContext)

    deserialization_context = DeserializationContext(message_type="tests.message")
    registry.decode(b"explicit", name="custom", context=deserialization_context)
    assert serializer.decoded_context is deserialization_context

    with pytest.raises(SerializationError, match="do not match"):
        registry.resolve(name="custom", content_type="application/json")


def test_plugin_registry_introspection_caching_unregister_and_alias() -> None:
    registry = PluginRegistry[object]()
    calls = 0

    def load() -> object:
        nonlocal calls
        calls += 1
        return {"plugin": True}

    registry.register("eager", 42, source="test")
    registry.register_lazy("lazy", load, source="fixture")

    assert "eager" in registry
    assert 42 not in registry
    assert len(registry) == 2
    assert tuple(registry) == ("eager", "lazy")
    assert registry.names == ("eager", "lazy")
    assert [item.loaded for item in registry.registrations] == [True, False]
    assert registry.get("eager") == 42
    assert registry.resolve("lazy") == {"plugin": True}
    assert registry.resolve("lazy") == {"plugin": True}
    assert calls == 1

    registration = registry.unregister("lazy")
    assert registration.name == "lazy"
    assert registration.source == "fixture"
    assert registration.loaded
    assert registration.value == {"plugin": True}


def test_plugin_registry_validation_and_loader_failures_are_typed() -> None:
    registry = PluginRegistry[object]()
    with pytest.raises(RegistryError, match="callable"):
        registry.register_lazy("bad", None)  # type: ignore[arg-type]
    with pytest.raises(RegistryError, match="must not be empty"):
        registry.register("   ", object())

    registry.register("existing", object())
    with pytest.raises(DuplicateRegistrationError):
        registry.register(" existing ", object())
    with pytest.raises(RegistryError, match="unknown plugin"):
        registry.resolve("missing")
    with pytest.raises(RegistryError, match="unknown plugin"):
        registry.unregister("missing")

    def fail() -> object:
        raise ValueError("secret details")

    registry.register_lazy("broken", fail, source="unit-test")
    with pytest.raises(PluginLoadError, match="ValueError") as caught:
        registry.resolve("broken")
    assert caught.value.context == {"plugin": "broken", "source": "unit-test"}
    assert "secret details" not in str(caught.value)

    expected = PluginLoadError("already translated")

    def translated_failure() -> object:
        raise expected

    registry.register_lazy("translated", translated_failure)
    with pytest.raises(PluginLoadError) as reraised:
        registry.resolve("translated")
    assert reraised.value is expected


def test_plugin_entry_point_discovery_ignore_duplicate_and_failure_modes() -> None:
    source = lambda: _plugin_source(  # noqa: E731
        EntryPoint(name="text", value="builtins:str", group=PluginGroups.SERIALIZERS),
        EntryPoint(
            name="broken",
            value="pyev_missing_plugin_for_test:serializer",
            group=PluginGroups.SERIALIZERS,
        ),
    )
    descriptors = discover_plugins(PluginGroups.SERIALIZERS, source=source)
    assert [descriptor.name for descriptor in descriptors] == ["broken", "text"]

    registry = PluginRegistry[object]()
    registry.register("text", bytes)
    assert register_discovered_plugins(
        registry,
        PluginGroups.SERIALIZERS,
        source=source,
        ignore_registered=True,
    ) == ("broken",)
    assert registry.resolve("text") is bytes
    with pytest.raises(PluginLoadError, match="broken"):
        registry.resolve("broken")

    duplicate_registry = PluginRegistry[object]()
    duplicate_registry.register("text", bytes)
    with pytest.raises(DuplicateRegistrationError):
        register_discovered_plugins(
            duplicate_registry,
            PluginGroups.SERIALIZERS,
            source=source,
        )


def test_discover_serializer_plugins_populates_the_injected_empty_registry() -> None:
    registry = PluginRegistry[object]()
    source = lambda: _plugin_source(  # noqa: E731
        EntryPoint(name="text", value="builtins:str", group=PluginGroups.SERIALIZERS)
    )

    result = discover_serializer_plugins(registry, source=source)

    assert result is registry
    assert result.resolve("text") is str


def test_broker_factory_accepts_mapping_validated_config_and_injected_dependencies() -> None:
    router = Router()
    from_mapping = BrokerFactory.create({"engine": "local", "source": "factory"}, router=router)
    validated = BrokerConfig.from_mapping({"engine": "memory", "source": "validated"})
    from_config = BrokerFactory.create(validated)

    assert isinstance(from_mapping, Broker)
    assert from_mapping.config.source == "factory"
    assert from_mapping.router is router
    assert from_mapping.state is BrokerState.NEW
    assert from_config.config is validated


def test_structured_logging_redacts_context_fields_payload_and_inline_credentials(
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter = get_logger(
        "pyev.tests.structured",
        context={"service": "orders", "api_key": "base-secret"},
    )
    assert isinstance(adapter, StructuredLogAdapter)

    with caplog.at_level(logging.INFO, logger="pyev.tests.structured"):
        adapter.event(
            "message.published",
            payload={"customer": "Alice"},
            password="event-secret",
            endpoint="redis://user:secret@localhost/0",
        )

    record = caplog.records[-1]
    assert record.message == "message.published"
    assert record.event == "message.published"  # type: ignore[attr-defined]
    assert record.service == "orders"  # type: ignore[attr-defined]
    assert record.api_key == REDACTED  # type: ignore[attr-defined]
    assert record.password == REDACTED  # type: ignore[attr-defined]
    assert record.payload == REDACTED  # type: ignore[attr-defined]
    assert record.endpoint == f"redis://{REDACTED}@localhost/0"  # type: ignore[attr-defined]


def test_structured_logging_can_include_redacted_payload_and_process_non_mapping_extra() -> None:
    logger = logging.getLogger("pyev.tests.process")
    adapter = StructuredLogAdapter(logger, {"token": "hidden"}, include_payload=True)

    message, kwargs = adapter.process(
        "published",
        {"extra": {"payload": {"password": "inside", "value": 1}}},
    )
    assert message == "published"
    assert kwargs["extra"] == {
        "token": REDACTED,
        "payload": {"password": REDACTED, "value": 1},
    }

    _, invalid_extra = adapter.process("event", {"extra": "not-a-mapping"})
    assert invalid_extra["extra"] == {"token": REDACTED}


def test_redaction_helpers_cover_assignments_extra_keys_sequences_and_depth() -> None:
    assert is_sensitive_key("database_password")
    assert is_sensitive_key("custom-secret", frozenset({"custom_secret"}))
    assert not is_sensitive_key("username")

    text = (
        "redis://user:secret@localhost password=hunter2 "
        "authorization: Bearer bearer-token api-key='key-value'"
    )
    redacted_text = redact_text(text)
    assert "user:secret" not in redacted_text
    assert "hunter2" not in redacted_text
    assert "bearer-token" not in redacted_text
    assert "key-value" not in redacted_text

    value = {
        "tuple": ("password=one", {"session": "two"}),
        "list": ["token=three"],
        "bytes": b"password=not-inspected",
    }
    redacted = redact_value(value, extra_keys=frozenset({"session"}))
    assert redacted == {
        "tuple": (f"password={REDACTED}", {"session": REDACTED}),
        "list": [f"token={REDACTED}"],
        "bytes": b"password=not-inspected",
    }
    assert redact_value({"nested": {"too": "deep"}}, max_depth=0) == {"nested": "[MAX_DEPTH]"}
    assert redact_mapping({"authorization": "Basic abc"}) == {"authorization": REDACTED}
