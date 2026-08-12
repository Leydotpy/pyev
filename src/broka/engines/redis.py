"""Optional Redis Pub/Sub and Streams transport engine.

The ``redis`` package is imported only when the engine connects. Merely importing
``pyev`` therefore never requires or initializes Redis.
"""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import time
from collections.abc import Coroutine, Mapping, Sequence
from contextlib import suppress
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from broka.capabilities import Capability, CapabilitySet
from broka.engines.base import (
    Availability,
    BaseEngine,
    EngineConsumer,
    EngineDeliveryCallback,
    EngineHealth,
    EngineIncomingMessage,
    EnginePublishContext,
    EnginePublishResult,
    EngineSubscription,
)
from broka.exceptions import UnsupportedCapabilityError


class _RedisConsumer(EngineConsumer):
    def __init__(self, consumer_id: str) -> None:
        self._id = consumer_id
        self._task: asyncio.Task[None] | None = None
        self._resumed = asyncio.Event()
        self._resumed.set()
        self._closed = False
        self.last_error: str | None = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def closed(self) -> bool:
        return self._closed

    def start(self, coroutine: Coroutine[Any, Any, None]) -> None:
        self._task = asyncio.create_task(coroutine, name=f"pyev-redis-{self.id}")

    async def pause(self) -> None:
        self._resumed.clear()

    async def resume(self) -> None:
        self._resumed.set()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._resumed.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None


class _PubSubAcknowledgement:
    async def ack(self) -> None:
        return None

    async def nack(self, requeue: bool = True) -> None:
        if requeue:
            raise UnsupportedCapabilityError(
                Capability.AT_LEAST_ONCE,
                operation="Redis Pub/Sub nack with requeue",
            )

    async def reject(self) -> None:
        return None

    async def requeue(self) -> None:
        raise UnsupportedCapabilityError(
            Capability.AT_LEAST_ONCE,
            operation="Redis Pub/Sub requeue",
        )

    async def defer(self, delay: float) -> None:
        del delay
        raise UnsupportedCapabilityError(
            Capability.NATIVE_DELAY,
            operation="Redis Pub/Sub defer",
        )

    async def touch(self, extension: float) -> None:
        del extension
        raise UnsupportedCapabilityError(
            Capability.VISIBILITY_TIMEOUT,
            operation="Redis Pub/Sub touch",
        )


class _StreamsAcknowledgement:
    def __init__(
        self,
        client: Any,
        *,
        stream: str,
        group: str,
        entry_id: str,
        payload: bytes,
        headers: Mapping[str, str],
    ) -> None:
        self.client = client
        self.stream = stream
        self.group = group
        self.entry_id = entry_id
        self.payload = payload
        self.headers = headers
        self._finished = False

    async def ack(self) -> None:
        if not self._finished:
            await self.client.xack(self.stream, self.group, self.entry_id)
            self._finished = True

    async def nack(self, requeue: bool = True) -> None:
        if requeue:
            await self.requeue()
        else:
            await self.reject()

    async def reject(self) -> None:
        await self.ack()

    async def requeue(self) -> None:
        if self._finished:
            return
        await self.client.xadd(
            self.stream,
            {"payload": self.payload, "headers": json.dumps(dict(self.headers))},
        )
        await self.ack()

    async def defer(self, delay: float) -> None:
        if delay < 0:
            raise ValueError("defer delay cannot be negative")
        await asyncio.sleep(delay)
        await self.requeue()

    async def touch(self, extension: float) -> None:
        del extension
        raise UnsupportedCapabilityError(
            Capability.VISIBILITY_TIMEOUT,
            operation="Redis Streams touch",
        )


class RedisEngine(BaseEngine):
    """Redis adapter supporting explicitly selected ``pubsub`` or ``streams`` mode."""

    name = "redis"
    priority = 50

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        super().__init__(config)
        self.mode = str(self.config.get("mode", "streams")).lower()
        if self.mode not in {"pubsub", "streams"}:
            raise ValueError("Redis mode must be 'pubsub' or 'streams'")
        raw_url = self.config.get("url", "redis://localhost:6379/0")
        if not isinstance(raw_url, str) or not raw_url.strip():
            raise ValueError("Redis url must not be empty")
        self.url = raw_url
        raw_max_length = self.config.get("max_length")
        self.max_length = (
            None if raw_max_length is None else _config_int(raw_max_length, "max_length")
        )
        if self.max_length is not None and self.max_length < 1:
            raise ValueError("Redis max_length must be a positive integer")
        self.claim_idle_ms = _config_int(self.config.get("claim_idle_ms", 60_000), "claim_idle_ms")
        if self.claim_idle_ms < 1:
            raise ValueError("Redis claim_idle_ms must be a positive integer")
        self.claim_interval = _config_float(
            self.config.get("claim_interval", max(1.0, self.claim_idle_ms / 2_000)),
            "claim_interval",
        )
        if self.claim_interval <= 0:
            raise ValueError("Redis claim_interval must be positive")
        self._client: Any = None
        self._consumers: dict[str, _RedisConsumer] = {}

    @classmethod
    def is_available(cls, config: Mapping[str, object] | None = None) -> Availability:
        try:
            dependency = importlib.util.find_spec("redis.asyncio")
        except (ImportError, ModuleNotFoundError, ValueError):
            dependency = None
        if dependency is None:
            return Availability(False, "install pyev[redis] to use the Redis engine")
        if config is not None and "url" in config and not config.get("url"):
            return Availability(False, "Redis engine configuration requires 'url'")
        return Availability(True)

    @property
    def capabilities(self) -> CapabilitySet:
        common = {
            Capability.PUBLISH_SUBSCRIBE,
            Capability.FANOUT,
            Capability.REQUEST_REPLY,
        }
        if self.mode == "pubsub":
            return CapabilitySet(
                {
                    **dict.fromkeys(common),
                    Capability.WILDCARD_SUBSCRIPTIONS: None,
                    Capability.AT_MOST_ONCE: {"redis_mode": "pubsub", "durable": False},
                }
            )
        return CapabilitySet(
            {
                **dict.fromkeys(common),
                Capability.DURABLE_SUBSCRIPTIONS: {"redis_mode": "streams"},
                Capability.CONSUMER_GROUPS: None,
                Capability.COMPETING_CONSUMERS: None,
                Capability.AT_LEAST_ONCE: {"pending_recovery": "xautoclaim"},
                Capability.MESSAGE_ORDERING: {"scope": "stream-entry-id"},
            }
        )

    async def connect(self) -> None:
        if self._client is not None:
            return
        if not self.is_available(self.config):
            raise RuntimeError("Redis client unavailable; install pyev[redis]")
        from redis.asyncio import Redis

        client = Redis.from_url(self.url, decode_responses=False)
        try:
            await _await_if_needed(client.ping())
        except BaseException:
            with suppress(Exception):
                await _close_redis_client(client)
            raise
        self._client = client

    async def disconnect(self) -> None:
        consumers = tuple(self._consumers.values())
        self._consumers.clear()
        await asyncio.gather(
            *(consumer.close() for consumer in consumers),
            return_exceptions=True,
        )
        client = self._client
        self._client = None
        if client is not None:
            await _close_redis_client(client)

    async def publish(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult:
        if self._client is None:
            raise RuntimeError("Redis engine is not connected")
        if self.mode == "pubsub":
            subscribers = await self._client.publish(destination, payload)
            return EnginePublishResult(
                accepted=True,
                transport_id=f"subscribers:{subscribers}",
            )
        fields = {
            "payload": payload,
            "headers": json.dumps(dict(context.headers), separators=(",", ":")),
            "message_id": context.message_id,
        }
        kwargs = (
            {"maxlen": self.max_length, "approximate": True} if self.max_length is not None else {}
        )
        entry_id = await self._client.xadd(destination, fields, **kwargs)
        if isinstance(entry_id, bytes):
            entry_id = entry_id.decode()
        return EnginePublishResult(transport_id=str(entry_id))

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        if self._client is None:
            raise RuntimeError("Redis engine is not connected")
        existing = self._consumers.get(subscription.id)
        if existing is not None and not existing.closed:
            raise ValueError(f"consumer {subscription.id!r} is already registered")
        self._consumers.pop(subscription.id, None)
        consumer = _RedisConsumer(subscription.id)
        self._consumers[subscription.id] = consumer
        if self.mode == "pubsub":
            consumer.start(self._run_pubsub(consumer, subscription, callback))
        else:
            consumer.start(self._run_stream(consumer, subscription, callback))
        return consumer

    async def _run_pubsub(
        self,
        consumer: _RedisConsumer,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> None:
        pubsub = self._client.pubsub()
        await pubsub.psubscribe(subscription.pattern)
        try:
            while True:
                await consumer._resumed.wait()
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if message is None:
                    await asyncio.sleep(0)
                    continue
                data = message["data"]
                if not isinstance(data, bytes):
                    data = str(data).encode()
                channel = message["channel"]
                if isinstance(channel, bytes):
                    channel = channel.decode()
                await callback(
                    EngineIncomingMessage(
                        destination=str(channel),
                        payload=data,
                        acknowledgement=_PubSubAcknowledgement(),
                        transport_metadata=MappingProxyType({"redis_mode": "pubsub"}),
                    )
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            consumer.last_error = f"{type(error).__name__}: {error}"
            raise
        finally:
            await pubsub.aclose()

    async def _run_stream(
        self,
        consumer: _RedisConsumer,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> None:
        stream = subscription.destination
        group = str(self.config.get("group", "pyev"))
        consumer_name = str(self.config.get("consumer_name", f"pyev-{uuid4().hex[:12]}"))
        try:
            await self._client.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as error:
            if "BUSYGROUP" not in str(error):
                raise
        try:
            claim_cursor = "0-0"
            next_claim_at = 0.0
            while True:
                await consumer._resumed.wait()
                entries: Sequence[tuple[object, Mapping[object, object]]] = ()
                reclaimed = False
                now = time.monotonic()
                if now >= next_claim_at:
                    response = await self._client.xautoclaim(
                        stream,
                        group,
                        consumer_name,
                        min_idle_time=self.claim_idle_ms,
                        start_id=claim_cursor,
                        count=subscription.capacity,
                    )
                    claim_cursor, entries = _parse_xautoclaim(response)
                    reclaimed = bool(entries)
                    if claim_cursor == "0-0" or not entries:
                        claim_cursor = "0-0"
                        next_claim_at = now + self.claim_interval
                if not entries:
                    batches = await self._client.xreadgroup(
                        group,
                        consumer_name,
                        {stream: ">"},
                        count=1,
                        block=1000,
                    )
                    entries = tuple(
                        entry
                        for _stream_name, stream_entries in batches
                        for entry in stream_entries
                    )
                for entry_id, fields in entries:
                    raw, headers, decoded_id = _decode_stream_entry(entry_id, fields)
                    acknowledgement = _StreamsAcknowledgement(
                        self._client,
                        stream=stream,
                        group=group,
                        entry_id=decoded_id,
                        payload=raw,
                        headers=headers,
                    )
                    await callback(
                        EngineIncomingMessage(
                            destination=stream,
                            payload=raw,
                            acknowledgement=acknowledgement,
                            headers=MappingProxyType(headers),
                            transport_metadata=MappingProxyType(
                                {
                                    "redis_mode": "streams",
                                    "stream_entry_id": decoded_id,
                                    "consumer_group": group,
                                    "reclaimed": reclaimed,
                                }
                            ),
                            attempt=2 if reclaimed else 1,
                        )
                    )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            consumer.last_error = f"{type(error).__name__}: {error}"
            raise

    async def healthcheck(self) -> EngineHealth:
        if self._client is None:
            return EngineHealth(self.name, connected=False, healthy=False)
        started = time.perf_counter()
        try:
            await self._client.ping()
        except Exception as error:
            return EngineHealth(
                self.name,
                connected=True,
                healthy=False,
                details=MappingProxyType({"error": type(error).__name__, "mode": self.mode}),
            )
        latency = (time.perf_counter() - started) * 1000
        errors = {
            consumer.id: consumer.last_error
            for consumer in self._consumers.values()
            if consumer.last_error is not None
        }
        return EngineHealth(
            self.name,
            connected=True,
            healthy=not errors,
            latency_ms=latency,
            details=MappingProxyType(
                {
                    "mode": self.mode,
                    "active_consumers": sum(
                        not consumer.closed for consumer in self._consumers.values()
                    ),
                    "errors": errors,
                }
            ),
        )


def _parse_xautoclaim(
    response: object,
) -> tuple[str, Sequence[tuple[object, Mapping[object, object]]]]:
    if not isinstance(response, (list, tuple)) or len(response) < 2:
        raise RuntimeError("Redis XAUTOCLAIM returned an invalid response")
    raw_cursor, raw_entries = response[0], response[1]
    cursor = raw_cursor.decode() if isinstance(raw_cursor, bytes) else str(raw_cursor)
    if not isinstance(raw_entries, (list, tuple)):
        raise RuntimeError("Redis XAUTOCLAIM entries must be a sequence")
    entries: list[tuple[object, Mapping[object, object]]] = []
    for entry in raw_entries:
        if (
            not isinstance(entry, (list, tuple))
            or len(entry) != 2
            or not isinstance(entry[1], Mapping)
        ):
            raise RuntimeError("Redis XAUTOCLAIM returned a malformed stream entry")
        entries.append((entry[0], entry[1]))
    return cursor, tuple(entries)


def _decode_stream_entry(
    entry_id: object,
    fields: Mapping[object, object],
) -> tuple[bytes, dict[str, str], str]:
    raw = fields.get(b"payload", fields.get("payload", b""))
    if not isinstance(raw, bytes):
        raw = str(raw).encode()
    raw_headers = fields.get(b"headers", fields.get("headers", "{}"))
    if isinstance(raw_headers, bytes):
        raw_headers = raw_headers.decode()
    parsed_headers = json.loads(str(raw_headers))
    if not isinstance(parsed_headers, Mapping):
        raise RuntimeError("Redis stream headers must decode to an object")
    headers = {str(key): str(value) for key, value in parsed_headers.items()}
    decoded_id = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
    return raw, headers, decoded_id


def _config_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"Redis {name} must be an integer")
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"Redis {name} must be an integer") from error


def _config_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError(f"Redis {name} must be numeric")
    try:
        return float(value)
    except ValueError as error:
        raise ValueError(f"Redis {name} must be numeric") from error


async def _close_redis_client(client: Any) -> None:
    await _await_if_needed(client.aclose())


async def _await_if_needed(result: object) -> None:
    if inspect.isawaitable(result):
        await result
