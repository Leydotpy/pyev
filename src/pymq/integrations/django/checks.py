"""Django system checks for the namespaced pyev configuration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pymq.exceptions import ConfigurationError
from pymq.integrations.django.config import DjangoSettingsProvider

_registered = False


def _configuration_check(**_kwargs: Any) -> list[Any]:
    from django.conf import settings
    from django.core.checks import Error, Warning

    raw = getattr(settings, "PYEV", {})
    if not isinstance(raw, Mapping):
        return [
            Error(
                "PYEV must be a mapping.",
                hint="Set PYEV = {'ENGINE': 'memory'} or another valid engine configuration.",
                id="pyev.E001",
            )
        ]

    try:
        config = DjangoSettingsProvider(settings).build(include_environment=False)
    except ConfigurationError as error:
        return [
            Error(
                f"PYEV configuration is invalid: {error}",
                hint="Correct the named setting path before starting the ASGI worker.",
                id="pyev.E002",
            )
        ]

    messages: list[Any] = []
    integration = DjangoSettingsProvider(settings).option("django", {})
    if isinstance(integration, Mapping) and integration.get("auto_start") is True:
        messages.append(
            Error(
                "PYEV.DJANGO.AUTO_START is unsafe and is not supported.",
                hint=(
                    "Wrap get_asgi_application() with "
                    "pyev.integrations.django.asgi.with_broker_lifespan instead."
                ),
                id="pyev.E003",
            )
        )
    if not getattr(settings, "DEBUG", False) and config.engine in {"local", "memory"}:
        messages.append(
            Warning(
                f"PYEV uses the process-local {config.engine!r} engine in deployment mode.",
                hint="Configure an external engine for communication across Django workers.",
                id="pyev.W001",
            )
        )
    if (
        isinstance(integration, Mapping)
        and integration.get("outbox_enabled") is True
        and integration.get("outbox_store") is None
    ):
        messages.append(
            Warning(
                "PYEV.DJANGO.OUTBOX_ENABLED is set without OUTBOX_STORE.",
                hint="Configure a durable OutboxStore factory or disable the outbox dispatcher.",
                id="pyev.W002",
            )
        )
    return messages


def register_checks() -> None:
    """Register configuration checks exactly once per Django process."""

    global _registered
    if _registered:
        return
    from django.core.checks import Tags, register

    register(Tags.compatibility, deploy=True)(_configuration_check)
    _registered = True


__all__ = ["register_checks"]
