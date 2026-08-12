"""Typed, immutable, composable configuration for :mod:`pyev`.

Configuration is never read during import. :class:`ConfigLoader` applies the
documented precedence, from lowest to highest: defaults, files, framework
settings, ``PYEV_*`` environment variables, then construction overrides.
"""

from __future__ import annotations

import json
import os
import re
import tomllib
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Final, Protocol, cast
from urllib.parse import SplitResult, urlsplit, urlunsplit

from pymq.exceptions import ConfigurationError

type ConfigScalar = str | int | float | bool | None
_MISSING: Final = object()
_SECRET_KEY_PARTS: Final = frozenset(
    {
        "authorization",
        "credential",
        "credentials",
        "password",
        "passwd",
        "private_key",
        "secret",
        "secret_key",
        "token",
        "api_key",
        "access_key",
    }
)


@dataclass(frozen=True, slots=True, repr=False)
class SecretValue:
    """A string-like secret whose representation never exposes its value."""

    _value: str

    def get_secret_value(self) -> str:
        """Reveal the value explicitly for an engine or credential provider."""

        return self._value

    reveal = get_secret_value

    def __repr__(self) -> str:
        return "SecretValue('********')"

    def __str__(self) -> str:
        return "********"

    def __bool__(self) -> bool:
        return bool(self._value)


def _is_secret_key(key: str) -> bool:
    key = key.casefold().replace("-", "_")
    if key in _SECRET_KEY_PARTS:
        return True
    parts = tuple(part for part in re.split(r"[._]", key) if part)
    return any(part in _SECRET_KEY_PARTS for part in parts)


def _redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        if not parsed.scheme or parsed.hostname is None or parsed.username is None:
            return value
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        netloc = f"********@{host}"
        redacted = SplitResult(parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
        return urlunsplit(redacted)
    except ValueError:
        # Even a malformed credential-bearing URL must never leak through repr.
        if "://" in value and "@" in value:
            scheme, remainder = value.split("://", 1)
            return f"{scheme}://********@{remainder.rsplit('@', 1)[-1]}"
        return value


def _freeze(value: object, *, key: str | None = None) -> object:
    if isinstance(value, SecretValue):
        return value
    if key is not None and _is_secret_key(key) and value is not None:
        return SecretValue(str(value))
    if isinstance(value, Mapping):
        return FrozenMapping(cast(Mapping[str, object], value))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value


def reveal_secret(value: object) -> object:
    """Recursively produce plain values, explicitly revealing secrets."""

    if isinstance(value, SecretValue):
        return value.get_secret_value()
    if isinstance(value, Mapping):
        return {str(key): reveal_secret(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [reveal_secret(item) for item in value]
    if isinstance(value, frozenset):
        return [reveal_secret(item) for item in value]
    return value


def redact(value: object, *, key: str | None = None) -> object:
    """Recursively return a log-safe representation of configuration data."""

    if isinstance(value, SecretValue):
        return "********"
    if key is not None and _is_secret_key(key) and value is not None:
        return "********"
    if isinstance(value, Mapping):
        return {str(item_key): redact(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, (tuple, list, frozenset, set)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return _redact_url(value)
    return value


class FrozenMapping(Mapping[str, object]):
    """A recursively immutable mapping with a redacted representation."""

    __slots__ = ("_data",)

    def __init__(self, values: Mapping[str, object] | None = None) -> None:
        self._data = {
            str(key): _freeze(value, key=str(key)) for key, value in (values or {}).items()
        }

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"FrozenMapping({redact(self._data)!r})"

    def as_dict(self, *, reveal_secrets: bool = False) -> dict[str, object]:
        """Return a detached dict, redacted unless explicitly requested."""

        converted = reveal_secret(self._data) if reveal_secrets else redact(self._data)
        return cast(dict[str, object], converted)

    def get_path(self, path: str | Iterable[str], default: object = None) -> object:
        """Read a dotted or segmented path without mutating configuration."""

        parts = path.split(".") if isinstance(path, str) else tuple(path)
        current: object = self
        for part in parts:
            if not isinstance(current, Mapping) or part not in current:
                return default
            current = current[part]
        return current


ConfigView = FrozenMapping


@dataclass(frozen=True, slots=True)
class SerializationSettings:
    """Core serializer selection settings."""

    default: str = "json"
    allowed: tuple[str, ...] = ("json",)
    options: FrozenMapping = field(default_factory=FrozenMapping)

    @classmethod
    def from_value(cls, value: object) -> SerializationSettings:
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            return cls(default=value, allowed=(value,))
        if value is None:
            return cls()
        if not isinstance(value, Mapping):
            raise _config_error(
                "serialization",
                value,
                "a string or mapping",
                "Set serialization to a serializer name or a settings mapping.",
            )
        default = value.get("default", "json")
        if not isinstance(default, str) or not default:
            raise _config_error(
                "serialization.default",
                default,
                "a non-empty string",
                "Choose a registered serializer such as 'json'.",
            )
        allowed_value = value.get("allowed", (default,))
        allowed: tuple[str, ...]
        if isinstance(allowed_value, str):
            allowed = (allowed_value,)
        elif isinstance(allowed_value, Iterable):
            allowed = tuple(str(item) for item in allowed_value)
        else:
            raise _config_error(
                "serialization.allowed",
                allowed_value,
                "a sequence of serializer names",
                "Provide a JSON/YAML array of serializer names.",
            )
        if default not in allowed:
            raise ConfigurationError(
                "configuration at 'serialization.allowed' must contain the default serializer; "
                "add the configured default serializer to the allowlist"
            )
        options = {
            str(key): item for key, item in value.items() if key not in {"default", "allowed"}
        }
        return cls(default=default, allowed=allowed, options=FrozenMapping(options))

    def as_dict(self, *, reveal_secrets: bool = False) -> dict[str, object]:
        return {
            "default": self.default,
            "allowed": list(self.allowed),
            **self.options.as_dict(reveal_secrets=reveal_secrets),
        }


def _config_error(path: str, value: object, expected: str, guidance: str) -> ConfigurationError:
    category = type(value).__name__
    return ConfigurationError(
        f"invalid configuration at {path!r}: received {category}, expected {expected}. {guidance}"
    )


def _require_mapping(value: object, path: str) -> Mapping[str, object]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise _config_error(path, value, "a mapping", f"Set {path} to an object/table.")
    return cast(Mapping[str, object], value)


@dataclass(frozen=True, slots=True, repr=False)
class BrokerConfig:
    """Validated immutable settings consumed by :class:`pyev.Broker`."""

    engine: str | None = "memory"
    engines: FrozenMapping = field(default_factory=FrozenMapping)
    serialization: SerializationSettings = field(default_factory=SerializationSettings)
    reliability: FrozenMapping = field(default_factory=FrozenMapping)
    middleware: FrozenMapping = field(default_factory=FrozenMapping)
    routing: FrozenMapping = field(default_factory=FrozenMapping)
    lifecycle: FrozenMapping = field(default_factory=FrozenMapping)
    source: str | None = None
    allow_memory_fallback: bool = False
    extra: FrozenMapping = field(default_factory=FrozenMapping)

    @property
    def default_engine(self) -> str | None:
        """Compatibility-friendly explicit name for the configured engine."""

        return self.engine

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, object] | BrokerConfig | None = None,
    ) -> BrokerConfig:
        """Validate one mapping without reading files or environment variables."""

        if isinstance(values, BrokerConfig):
            return values
        data = {} if values is None else dict(values)
        engine = data.get("engine", data.get("default_engine", "memory"))
        if engine is not None and (not isinstance(engine, str) or not engine.strip()):
            raise _config_error(
                "engine",
                engine,
                "a non-empty engine name or null",
                "Set engine to a registered name such as 'memory'.",
            )
        engines_mapping = _require_mapping(data.get("engines", {}), "engines")
        normalized_engines = {
            str(name): _require_mapping(engine_config, f"engines.{name}")
            for name, engine_config in engines_mapping.items()
        }

        source = data.get("source")
        if source is not None and not isinstance(source, str):
            raise _config_error(
                "source",
                source,
                "a string or null",
                "Use a stable application or service identifier.",
            )
        fallback = data.get("allow_memory_fallback", False)
        if not isinstance(fallback, bool):
            raise _config_error(
                "allow_memory_fallback",
                fallback,
                "a boolean",
                "Use true only when an in-process fallback is acceptable.",
            )

        known = {
            "engine",
            "default_engine",
            "engines",
            "serialization",
            "reliability",
            "middleware",
            "routing",
            "lifecycle",
            "source",
            "allow_memory_fallback",
        }
        return cls(
            engine=engine,
            engines=FrozenMapping(normalized_engines),
            serialization=SerializationSettings.from_value(data.get("serialization")),
            reliability=FrozenMapping(_require_mapping(data.get("reliability", {}), "reliability")),
            middleware=FrozenMapping(_require_mapping(data.get("middleware", {}), "middleware")),
            routing=FrozenMapping(_require_mapping(data.get("routing", {}), "routing")),
            lifecycle=FrozenMapping(_require_mapping(data.get("lifecycle", {}), "lifecycle")),
            source=source,
            allow_memory_fallback=fallback,
            extra=FrozenMapping({key: value for key, value in data.items() if key not in known}),
        )

    @classmethod
    def load(
        cls,
        *,
        defaults: object = None,
        files: str | Path | Iterable[str | Path] | None = None,
        framework: object = None,
        environ: Mapping[str, str] | None = None,
        overrides: object = None,
        env_prefix: str = "PYEV_",
        include_environment: bool = True,
    ) -> BrokerConfig:
        """Load and merge all supported configuration layers."""

        return ConfigLoader(env_prefix=env_prefix).load(
            defaults=defaults,
            files=files,
            framework=framework,
            environ=environ,
            overrides=overrides,
            include_environment=include_environment,
        )

    def engine_settings(self, name: str | None = None) -> ConfigView:
        """Return immutable settings for one configured engine."""

        selected = name or self.engine
        if selected is None:
            return FrozenMapping()
        value = self.engines.get(selected, {})
        if not isinstance(value, Mapping):
            raise _config_error(
                f"engines.{selected}",
                value,
                "a mapping",
                "Correct the selected engine's configuration.",
            )
        return value if isinstance(value, FrozenMapping) else FrozenMapping(value)

    def as_dict(self, *, reveal_secrets: bool = False) -> dict[str, object]:
        """Return a detached mapping, redacted by default."""

        data: dict[str, object] = {
            "engine": self.engine,
            "engines": self.engines.as_dict(reveal_secrets=reveal_secrets),
            "serialization": self.serialization.as_dict(reveal_secrets=reveal_secrets),
            "reliability": self.reliability.as_dict(reveal_secrets=reveal_secrets),
            "middleware": self.middleware.as_dict(reveal_secrets=reveal_secrets),
            "routing": self.routing.as_dict(reveal_secrets=reveal_secrets),
            "lifecycle": self.lifecycle.as_dict(reveal_secrets=reveal_secrets),
            "source": self.source,
            "allow_memory_fallback": self.allow_memory_fallback,
        }
        data.update(self.extra.as_dict(reveal_secrets=reveal_secrets))
        return data

    def with_overrides(self, overrides: Mapping[str, object]) -> BrokerConfig:
        """Create a new config with deterministic deep construction overrides."""

        base = self.as_dict(reveal_secrets=True)
        return type(self).from_mapping(deep_merge(base, overrides))

    def __repr__(self) -> str:
        return f"BrokerConfig({self.as_dict()!r})"


class ConfigProvider(Protocol):
    """Structural interface for framework and third-party config providers."""

    def load(self) -> Mapping[str, object]: ...


def _as_mapping(source: object, *, label: str) -> Mapping[str, object]:
    if source is None:
        return {}
    if isinstance(source, BrokerConfig):
        return source.as_dict(reveal_secrets=True)
    if isinstance(source, Mapping):
        return cast(Mapping[str, object], source)
    model_dump = getattr(source, "model_dump", None)
    if callable(model_dump):
        result = model_dump()
        return _require_mapping(result, label)
    load = getattr(source, "load", None)
    if callable(load):
        result = load()
        return _require_mapping(result, label)
    for attribute in ("PYEV", "PYEV_CONFIG"):
        if hasattr(source, attribute):
            return _require_mapping(getattr(source, attribute), f"{label}.{attribute}")
    raise _config_error(
        label,
        source,
        "a mapping, BrokerConfig, Pydantic model, or ConfigProvider",
        "Adapt framework settings to a mapping or implement load().",
    )


def deep_merge(*sources: Mapping[str, object]) -> dict[str, object]:
    """Recursively merge mappings; later values replace earlier values."""

    result: dict[str, object] = {}
    for source in sources:
        for key, value in source.items():
            current = result.get(str(key), _MISSING)
            if isinstance(current, Mapping) and isinstance(value, Mapping):
                result[str(key)] = deep_merge(
                    cast(Mapping[str, object], current),
                    cast(Mapping[str, object], value),
                )
            else:
                result[str(key)] = value
    return result


def read_config_file(path: str | Path) -> Mapping[str, object]:
    """Read JSON, TOML, or YAML from an explicitly supplied path."""

    config_path = Path(path)
    try:
        content = config_path.read_bytes()
    except OSError as error:
        raise ConfigurationError(
            f"unable to read configuration file {config_path}: {error}"
        ) from error
    suffix = config_path.suffix.casefold()
    try:
        if suffix == ".json":
            value = json.loads(content)
        elif suffix == ".toml":
            value = tomllib.loads(content.decode("utf-8"))
        elif suffix in {".yaml", ".yml"}:
            try:
                yaml = import_module("yaml")
            except ImportError as error:
                raise ConfigurationError(
                    "YAML configuration requires the optional 'PyYAML' package; "
                    "install pyev with its yaml extra or use TOML/JSON"
                ) from error
            safe_load = cast(Callable[[bytes], object], yaml.__dict__["safe_load"])
            value = safe_load(content)
        else:
            raise ConfigurationError(
                f"unsupported configuration file type {suffix!r}; use .toml, .json, .yaml, or .yml"
            )
    except (UnicodeDecodeError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        raise ConfigurationError(
            f"invalid {suffix.removeprefix('.')} configuration: {error}"
        ) from error
    return _require_mapping(value, str(config_path))


def _parse_environment_value(value: str) -> object:
    stripped = value.strip()
    if not stripped:
        return ""
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def environment_config(
    environ: Mapping[str, str] | None = None,
    *,
    prefix: str = "PYEV_",
) -> dict[str, object]:
    """Convert ``PYEV_*`` variables to a nested mapping.

    Double underscores delimit nesting, for example
    ``PYEV_ENGINES__REDIS__URL``. Values use JSON scalar/array/object parsing
    where valid and otherwise remain strings.
    """

    source = os.environ if environ is None else environ
    result: dict[str, object] = {}
    for name in sorted(source):
        if not name.startswith(prefix) or name == prefix:
            continue
        parts = [part.casefold() for part in name[len(prefix) :].split("__") if part]
        if not parts:
            continue
        cursor = result
        for part in parts[:-1]:
            existing = cursor.get(part)
            if not isinstance(existing, dict):
                existing = {}
                cursor[part] = existing
            cursor = existing
        cursor[parts[-1]] = _parse_environment_value(source[name])
    return result


class ConfigLoader:
    """Explicit configuration provider orchestrator."""

    def __init__(self, *, env_prefix: str = "PYEV_") -> None:
        if not env_prefix:
            raise ConfigurationError("environment prefix must not be empty")
        self.env_prefix = env_prefix

    def load(
        self,
        *,
        defaults: object = None,
        files: str | Path | Iterable[str | Path] | None = None,
        framework: object = None,
        environ: Mapping[str, str] | None = None,
        overrides: object = None,
        include_environment: bool = True,
    ) -> BrokerConfig:
        """Apply defaults < files < framework < environment < overrides."""

        layers: list[Mapping[str, object]] = [_as_mapping(defaults, label="defaults")]
        if files is not None:
            paths = (files,) if isinstance(files, (str, Path)) else tuple(files)
            layers.extend(read_config_file(path) for path in paths)
        layers.append(_as_mapping(framework, label="framework"))
        if include_environment:
            layers.append(environment_config(environ, prefix=self.env_prefix))
        layers.append(_as_mapping(overrides, label="overrides"))
        return BrokerConfig.from_mapping(deep_merge(*layers))


def load_config(**kwargs: object) -> BrokerConfig:
    """Convenience wrapper around :meth:`BrokerConfig.load`."""

    return BrokerConfig.load(**kwargs)  # type: ignore[arg-type]


__all__ = [
    "BrokerConfig",
    "ConfigLoader",
    "ConfigProvider",
    "ConfigView",
    "FrozenMapping",
    "SecretValue",
    "SerializationSettings",
    "deep_merge",
    "environment_config",
    "load_config",
    "read_config_file",
    "redact",
    "reveal_secret",
]
