"""Subscription validation and lifecycle edge cases."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from broka.delivery import Delivery
from broka.exceptions import InvalidStateTransitionError, LifecycleError
from broka.subscription import (
    Subscription,
    SubscriptionOptions,
    SubscriptionState,
)


async def _handler(_delivery: Delivery[object]) -> None:
    return None


class RecordingController:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def pause(self, _subscription: Subscription[Any]) -> None:
        self.calls.append("pause")

    async def resume(self, _subscription: Subscription[Any]) -> None:
        self.calls.append("resume")

    async def close(self, _subscription: Subscription[Any]) -> None:
        self.calls.append("close")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("concurrency", 0),
        ("capacity", False),
        ("prefetch", 0),
        ("max_in_flight", -1),
        ("consumer_group", ""),
        ("consumer_id", "   "),
    ],
)
def test_subscription_options_reject_invalid_values(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        SubscriptionOptions(**{field: value})


def test_subscription_options_freeze_detached_metadata() -> None:
    source = {"tenant": "one"}
    options = SubscriptionOptions(metadata=source)
    source["tenant"] = "two"
    assert options.metadata == {"tenant": "one"}
    with pytest.raises(TypeError):
        options.metadata["tenant"] = "three"  # type: ignore[index]


def test_subscription_constructor_validates_handler_identity_and_timestamp() -> None:
    with pytest.raises(TypeError, match="handler"):
        Subscription("route", None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="id"):
        Subscription("route", _handler, id="")
    with pytest.raises(ValueError, match="timezone-aware"):
        Subscription("route", _handler, created_at=datetime(2026, 1, 1))


async def test_subscription_controller_lifecycle_is_idempotent() -> None:
    controller = RecordingController()
    subscription = Subscription("orders.*", _handler, controller=controller)
    assert "created" in repr(subscription)
    await subscription.activate()
    await subscription.activate()
    assert subscription.active
    await subscription.pause()
    await subscription.pause()
    await subscription.resume()
    await subscription.resume()
    await subscription.close()
    await subscription.close()
    assert subscription.closed
    assert controller.calls == ["pause", "resume", "close"]
    with pytest.raises(InvalidStateTransitionError):
        await subscription.fail()


async def test_subscription_requires_controller_for_runtime_operations() -> None:
    subscription = Subscription("orders.*", _handler)
    await subscription.activate()
    with pytest.raises(LifecycleError, match="no controller"):
        await subscription.pause()

    paused = Subscription("orders.*", _handler, state=SubscriptionState.PAUSED)
    with pytest.raises(LifecycleError, match="no controller"):
        await paused.resume()


async def test_subscription_context_manager_activates_and_closes() -> None:
    controller = RecordingController()
    subscription = Subscription("orders.*", _handler, controller=controller)
    async with subscription as entered:
        assert entered is subscription
        assert subscription.active
    assert subscription.closed
    assert controller.calls == ["close"]


async def test_subscription_invalid_transitions_raise_typed_errors() -> None:
    subscription = Subscription("orders.*", _handler)
    with pytest.raises(InvalidStateTransitionError):
        await subscription.resume()
    await subscription.fail()
    assert subscription.state is SubscriptionState.FAILED
    with pytest.raises(InvalidStateTransitionError):
        await subscription.activate()
