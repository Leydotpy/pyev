"""Django application configuration for optional pyev checks."""

from __future__ import annotations

from django.apps import AppConfig


class PyevConfig(AppConfig):
    """Register checks without opening broker connections during app import."""

    name = "pyev.integrations.django"
    label = "pyev"
    verbose_name = "pyev message broker"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from pyev.integrations.django.checks import register_checks

        register_checks()
