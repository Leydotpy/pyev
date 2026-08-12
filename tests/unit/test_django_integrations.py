# ruff: noqa: E402
from __future__ import annotations

import io
import json
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast

import pytest

# Django must be configured before importing its ORM-backed optional integration.
django = pytest.importorskip("django")

from django.conf import settings

if not settings.configured:
    settings.configure(
        SECRET_KEY="pyev-tests",
        DEBUG=True,
        INSTALLED_APPS=["pyev.integrations.django"],
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        USE_TZ=True,
        PYEV={"ENGINE": "memory"},
    )
    django.setup()

from django.core.management import call_command
from django.db import connection, transaction
from django.test import override_settings

from pymq.broker import Broker
from pymq.envelope import Envelope
from pymq.integrations.django import clear_broker, publish_on_commit
from pymq.integrations.django.asgi import DjangoBrokerASGI
from pymq.integrations.django.checks import _configuration_check
from pymq.integrations.django.config import DjangoSettingsProvider
from pymq.integrations.django.models import AbstractOutboxRecord
from pymq.integrations.django.outbox import (
    DjangoModelOutboxStore,
    EnvelopeOutboxCodec,
    configure_outbox_store,
)
from pymq.reliability.outbox import MemoryOutboxStore, OutboxMessage


class RecordingPublisher:
    def __init__(self) -> None:
        self.messages: list[tuple[object, object]] = []

    async def publish(self, message: object, **kwargs: object) -> object:
        self.messages.append((message, kwargs.get("route")))
        return object()


class LifecycleProbe:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def startup(self) -> None:
        self.calls.append("startup")

    async def shutdown(self) -> None:
        self.calls.append("shutdown")


@pytest.fixture(autouse=True)
def reset_integration_state() -> Any:
    clear_broker()
    configure_outbox_store(None)
    yield
    clear_broker()
    configure_outbox_store(None)


def test_settings_provider_normalises_django_keys_and_applies_environment() -> None:
    source = SimpleNamespace(
        PYEV={
            "ENGINE": "memory",
            "ENGINES": {"MEMORY": {"CAPACITY": 12}},
            "DJANGO": {"OUTBOX_ENABLED": True},
        }
    )
    provider = DjangoSettingsProvider(source)

    config = provider.build(environ={"PYEV_ENGINE": "local"})

    assert config.engine == "local"
    assert provider.option("django.outbox_enabled") is True
    memory = cast(Mapping[str, object], config.engines["memory"])
    assert memory["capacity"] == 12


def test_publish_on_commit_runs_after_commit_and_skips_rollback() -> None:
    publisher = RecordingPublisher()

    with transaction.atomic():
        publish_on_commit(
            {"id": 1},
            broker=cast(Broker, publisher),
            route="users.created",
        )
        assert publisher.messages == []

    assert publisher.messages == [({"id": 1}, "users.created")]

    with pytest.raises(RuntimeError), transaction.atomic():
        publish_on_commit(
            {"id": 2},
            broker=cast(Broker, publisher),
            route="users.created",
        )
        raise RuntimeError("rollback")

    assert publisher.messages == [({"id": 1}, "users.created")]


@pytest.mark.asyncio
async def test_django_asgi_wrapper_owns_lifespan_without_calling_django_app() -> None:
    probe = LifecycleProbe()
    app_calls: list[str] = []
    messages = iter(
        [
            {"type": "lifespan.startup"},
            {"type": "lifespan.shutdown"},
        ]
    )
    sent: list[dict[str, object]] = []

    async def app(_scope: object, _receive: object, _send: object) -> None:
        app_calls.append("called")

    async def receive() -> dict[str, object]:
        return next(messages)

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    integration = DjangoBrokerASGI(cast(Any, app), probe)
    await integration({"type": "lifespan"}, receive, send)

    assert probe.calls == ["startup", "shutdown"]
    assert app_calls == []
    assert [item["type"] for item in sent] == [
        "lifespan.startup.complete",
        "lifespan.shutdown.complete",
    ]


def test_app_uses_only_abstract_outbox_model_and_codec_round_trips() -> None:
    assert AbstractOutboxRecord._meta.abstract is True
    envelope = Envelope.create(
        {"user_id": 42},
        type="users.created",
        source="tests",
    )

    decoded = EnvelopeOutboxCodec().decode(EnvelopeOutboxCodec().encode(envelope))

    assert decoded == envelope


def test_django_outbox_insert_participates_in_application_transaction() -> None:
    class TestOutboxRecord(AbstractOutboxRecord):
        class Meta:
            app_label = "pyev"

    envelope = Envelope.create(
        {"invoice_id": 7},
        type="billing.invoice.created",
        source="tests",
    )
    message = OutboxMessage(envelope, "billing.invoice.created")
    store = DjangoModelOutboxStore(TestOutboxRecord)

    with connection.schema_editor() as editor:
        editor.create_model(TestOutboxRecord)
    try:
        with pytest.raises(RuntimeError), transaction.atomic():
            store.add_in_transaction(message)
            raise RuntimeError("roll back domain change")
        assert TestOutboxRecord.objects.count() == 0

        with transaction.atomic():
            store.add_in_transaction(message)
        row = TestOutboxRecord.objects.get()
        assert row.destination == "billing.invoice.created"
        assert store.codec.decode(bytes(row.payload)) == envelope
    finally:
        with connection.schema_editor() as editor:
            editor.delete_model(TestOutboxRecord)


def test_system_check_reports_invalid_namespace_and_unsafe_autostart() -> None:
    with override_settings(PYEV="invalid"):
        errors = _configuration_check()
    assert [error.id for error in errors] == ["pyev.E001"]

    with override_settings(PYEV={"ENGINE": "memory", "DJANGO": {"AUTO_START": True}}):
        errors = _configuration_check()
    assert "pyev.E003" in {error.id for error in errors}


def test_namespaced_health_and_ping_management_commands() -> None:
    health_output = io.StringIO()
    call_command("pyev_health", stdout=health_output)
    health = json.loads(health_output.getvalue())
    assert health["ready"] is True
    assert health["engine"] == "memory"

    ping_output = io.StringIO()
    call_command("pyev_ping", stdout=ping_output)
    ping = json.loads(ping_output.getvalue())
    assert ping["accepted"] is True
    assert ping["route"] == "pyev.system.ping"


def test_outbox_management_command_inspects_configured_store() -> None:
    configure_outbox_store(MemoryOutboxStore())
    output = io.StringIO()

    call_command("pyev_outbox", "list", stdout=output)

    assert json.loads(output.getvalue()) == []
