"""Opt-in smoke tests against real external broker services."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from uuid import uuid4

import pytest

from pymq import Broker, Delivery

pytestmark = pytest.mark.integration


def _environment(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"set {name} to run this external-service test")
    return value


async def _exercise(config: Mapping[str, object], route: str) -> None:
    delivered = asyncio.Event()
    payloads: list[object] = []

    async def handler(delivery: Delivery[object]) -> None:
        payloads.append(delivery.message)
        delivered.set()

    async with Broker.from_config(config) as broker:
        subscription = await broker.subscribe(route, handler)
        await broker.publish({"transport": config["engine"]}, route=route)
        async with asyncio.timeout(20):
            await delivered.wait()
        await subscription.close()

    assert payloads == [{"transport": config["engine"]}]


async def test_redis_streams_real_service() -> None:
    url = _environment("PYEV_TEST_REDIS_URL")
    token = uuid4().hex
    await _exercise(
        {
            "engine": "redis",
            "engines": {
                "redis": {
                    "url": url,
                    "mode": "streams",
                    "group": f"pyev-integration-{token}",
                    "consumer_name": "test-consumer",
                }
            },
        },
        f"pyev.integration.redis.{token}",
    )


async def test_rabbitmq_real_service() -> None:
    url = _environment("PYEV_TEST_RABBITMQ_URL")
    token = uuid4().hex
    await _exercise(
        {
            "engine": "rabbitmq",
            "engines": {
                "rabbitmq": {
                    "url": url,
                    "exchange": f"pyev.integration.{token}",
                    "queue": f"pyev.integration.{token}",
                    "durable": False,
                    "auto_delete": True,
                }
            },
        },
        f"pyev.integration.rabbitmq.{token}",
    )


async def test_kafka_real_service() -> None:
    servers = _environment("PYEV_TEST_KAFKA_BOOTSTRAP_SERVERS")
    topic = _environment("PYEV_TEST_KAFKA_TOPIC")
    await _exercise(
        {
            "engine": "kafka",
            "engines": {
                "kafka": {
                    "bootstrap_servers": servers,
                    "group_id": f"pyev-integration-{uuid4().hex}",
                    "auto_offset_reset": "latest",
                }
            },
        },
        topic,
    )
