"""Django settings adapter for :mod:`pyev`.

The adapter is deliberately loaded from the optional Django namespace.  Core
configuration code therefore never imports Django, while Django projects still
get deterministic ``settings < environment < runtime overrides`` precedence.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from pymq.config import BrokerConfig, ConfigLoader
from pymq.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pymq.broker import Broker


def _normalise(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key).casefold(): _normalise(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalise(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalise(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class DjangoSettingsProvider:
    """Read the namespaced ``PYEV`` Django setting without import side effects."""

    settings_object: object | None = None
    namespace: str = "PYEV"

    def load(self) -> Mapping[str, object]:
        """Return a detached, lower-case-key configuration mapping."""

        settings = self.settings_object
        if settings is None:
            try:
                from django.conf import settings as django_settings
            except ImportError as error:  # pragma: no cover - optional dependency
                raise RuntimeError("Django integration requires the 'django' extra") from error
            settings = django_settings

        try:
            raw = getattr(settings, self.namespace, {})
        except Exception as error:
            raise ConfigurationError(
                "Django settings are not configured; configure DJANGO_SETTINGS_MODULE "
                "before constructing the pyev broker"
            ) from error
        if not isinstance(raw, Mapping):
            raise ConfigurationError(f"Django setting {self.namespace} must be a mapping")
        normalised = _normalise(raw)
        return cast(Mapping[str, object], normalised)

    def build(
        self,
        *,
        overrides: Mapping[str, object] | None = None,
        environ: Mapping[str, str] | None = None,
        include_environment: bool = True,
    ) -> BrokerConfig:
        """Build validated settings with environment and runtime precedence."""

        return ConfigLoader().load(
            framework=self.load(),
            environ=environ,
            overrides=overrides,
            include_environment=include_environment,
        )

    def option(self, path: str, default: object = None) -> object:
        """Read a dotted integration-specific setting from ``PYEV``."""

        current: object = self.load()
        for part in path.casefold().split("."):
            if not isinstance(current, Mapping) or part not in current:
                return default
            current = current[part]
        return current

    def create_broker(self) -> Broker:
        """Create this process's broker, optionally through ``DJANGO.BROKER_FACTORY``.

        A custom factory receives the validated :class:`BrokerConfig`.  This is
        the supported way to inject persistent DLQ/outbox services into Django
        worker and management-command processes.
        """

        from pymq.broker import Broker

        config = self.build()
        factory = self.option("django.broker_factory")
        if factory is None:
            return Broker.from_config(config)
        if isinstance(factory, str):
            try:
                from django.utils.module_loading import import_string

                factory = import_string(factory)
            except (ImportError, AttributeError) as error:
                raise ConfigurationError(
                    "PYEV.DJANGO.BROKER_FACTORY must be an importable callable"
                ) from error
        if not callable(factory):
            raise ConfigurationError("PYEV.DJANGO.BROKER_FACTORY must be callable")
        broker = factory(config)
        if not isinstance(broker, Broker):
            raise ConfigurationError("PYEV.DJANGO.BROKER_FACTORY must return a Broker")
        return broker


def load_django_config(
    *,
    overrides: Mapping[str, object] | None = None,
    environ: Mapping[str, str] | None = None,
    include_environment: bool = True,
) -> BrokerConfig:
    """Convenience wrapper around :class:`DjangoSettingsProvider`."""

    return DjangoSettingsProvider().build(
        overrides=overrides,
        environ=environ,
        include_environment=include_environment,
    )


__all__ = ["DjangoSettingsProvider", "load_django_config"]
