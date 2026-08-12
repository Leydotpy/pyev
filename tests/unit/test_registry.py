from __future__ import annotations

from collections.abc import Mapping

import pytest

from pymq.capabilities import CapabilitySet
from pymq.config import BrokerConfig
from pymq.engines.base import (
    Availability,
    BaseEngine,
    EngineConsumer,
    EngineDeliveryCallback,
    EngineHealth,
    EnginePublishContext,
    EnginePublishResult,
    EngineSubscription,
)
from pymq.exceptions import DuplicateRegistrationError, EngineUnavailableError
from pymq.registry import EngineRegistry, create_default_registry, default_engine_registry


class SampleEngine(BaseEngine):
    name = "test"
    priority = 25

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet.empty()

    async def connect(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def publish(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult:
        del destination, payload, context
        return EnginePublishResult()

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        del subscription, callback
        raise NotImplementedError

    async def healthcheck(self) -> EngineHealth:
        return EngineHealth(engine=self.name, connected=False, healthy=True)


class UnavailableEngine(SampleEngine):
    name = "unavailable"

    @classmethod
    def is_available(cls, config: Mapping[str, object] | None = None) -> Availability:
        del config
        return Availability(False, "test dependency is absent")


def test_registries_are_isolated_and_detect_duplicates() -> None:
    first = EngineRegistry()
    second = EngineRegistry()
    first.register(SampleEngine)

    assert first.names == ("test",)
    assert second.names == ()
    with pytest.raises(DuplicateRegistrationError):
        first.register(SampleEngine)


def test_lazy_registration_loads_once_and_can_be_unregistered() -> None:
    registry = EngineRegistry()
    calls = 0

    def loader() -> type[BaseEngine]:
        nonlocal calls
        calls += 1
        return SampleEngine

    registration = registry.register_lazy("test", loader, priority=25)

    assert not registration.loaded
    assert calls == 0
    assert registry.resolve("test") is SampleEngine
    assert registry.resolve("test") is SampleEngine
    assert calls == 1
    assert registry.unregister("test").loaded


def test_explicit_unavailable_engine_never_falls_back_silently() -> None:
    registry = EngineRegistry()
    registry.register(UnavailableEngine)

    with pytest.raises(EngineUnavailableError, match="test dependency is absent"):
        registry.select(BrokerConfig.from_mapping({"engine": "unavailable"}))


def test_default_registry_factory_returns_fresh_lazy_external_registries() -> None:
    first = create_default_registry()
    second = default_engine_registry()

    assert first is not second
    assert {"local", "memory", "redis", "rabbitmq", "kafka"} <= set(first.names)
    for registration in first.registrations:
        if registration.name in {"redis", "rabbitmq", "kafka"}:
            assert not registration.loaded
