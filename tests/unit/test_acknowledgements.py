"""Unified acknowledgement-adapter contract tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from pyev.acknowledgements import (
    BaseAcknowledgementAdapter,
    CallbackAcknowledgementAdapter,
    NoOpAcknowledgementAdapter,
)
from pyev.exceptions import UnsupportedCapabilityError


@pytest.mark.parametrize(
    ("operation", "arguments"),
    [
        ("ack", ()),
        ("nack", (False,)),
        ("reject", ()),
        ("requeue", ()),
        ("defer", (1.0,)),
        ("touch", (2.0,)),
    ],
)
async def test_base_adapter_fails_explicitly(
    operation: str,
    arguments: tuple[object, ...],
) -> None:
    adapter = BaseAcknowledgementAdapter()
    callback = getattr(adapter, operation)
    with pytest.raises(UnsupportedCapabilityError, match=operation):
        await callback(*arguments)


async def test_noop_adapter_supports_local_state_operations() -> None:
    adapter = NoOpAcknowledgementAdapter()
    await adapter.ack()
    await adapter.nack(requeue=False)
    await adapter.reject()
    await adapter.requeue()
    await adapter.defer(0.01)
    await adapter.touch(1.0)


async def test_callback_adapter_forwards_every_operation() -> None:
    calls: list[tuple[str, object | None]] = []

    async def no_argument(name: str) -> None:
        calls.append((name, None))

    async def with_bool(value: bool) -> None:
        calls.append(("nack", value))

    async def with_float(name: str, value: float) -> None:
        calls.append((name, value))

    def bind(name: str) -> Callable[[], Awaitable[None]]:
        async def callback() -> None:
            await no_argument(name)

        return callback

    async def defer(value: float) -> None:
        await with_float("defer", value)

    async def touch(value: float) -> None:
        await with_float("touch", value)

    adapter = CallbackAcknowledgementAdapter(
        ack=bind("ack"),
        nack=with_bool,
        reject=bind("reject"),
        requeue=bind("requeue"),
        defer=defer,
        touch=touch,
    )
    await adapter.ack()
    await adapter.nack(False)
    await adapter.reject()
    await adapter.requeue()
    await adapter.defer(3.0)
    await adapter.touch(4.0)

    assert calls == [
        ("ack", None),
        ("nack", False),
        ("reject", None),
        ("requeue", None),
        ("defer", 3.0),
        ("touch", 4.0),
    ]


async def test_callback_adapter_never_treats_missing_callbacks_as_noops() -> None:
    adapter = CallbackAcknowledgementAdapter()
    with pytest.raises(UnsupportedCapabilityError):
        await adapter.ack()
    with pytest.raises(UnsupportedCapabilityError):
        await adapter.nack()
    with pytest.raises(UnsupportedCapabilityError):
        await adapter.reject()
    with pytest.raises(UnsupportedCapabilityError):
        await adapter.requeue()
    with pytest.raises(UnsupportedCapabilityError):
        await adapter.defer(1.0)
    with pytest.raises(UnsupportedCapabilityError):
        await adapter.touch(1.0)
