"""Dependency-injection identity regressions for the broker facade."""

from pyev.broker import Broker
from pyev.event import EventRegistry
from pyev.middleware import InboundMiddlewarePipeline, OutboundMiddlewarePipeline
from pyev.registry import EngineRegistry
from pyev.routing import Router
from pyev.serialization import SerializerRegistry


def test_broker_preserves_explicitly_injected_empty_registries_and_pipelines() -> None:
    engines = EngineRegistry()
    events = EventRegistry()
    serializers = SerializerRegistry()
    router = Router()
    inbound: InboundMiddlewarePipeline[object, object] = InboundMiddlewarePipeline()
    outbound: OutboundMiddlewarePipeline[object, object] = OutboundMiddlewarePipeline()

    broker = Broker(
        registry=engines,
        event_registry=events,
        serializer_registry=serializers,
        router=router,
        inbound=inbound,
        outbound=outbound,
    )

    assert broker.registry is engines
    assert broker.event_registry is events
    assert broker.serializers is serializers
    assert broker.router is router
    assert broker.inbound_middleware is inbound
    assert broker.outbound_middleware is outbound
