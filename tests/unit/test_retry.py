from __future__ import annotations

import asyncio

import pytest

from pymq.exceptions import RetryExhaustedError
from pymq.reliability import (
    FixedBackoff,
    RetryBudget,
    RetryContext,
    RetryManager,
    RetryPolicy,
    TypeExceptionClassifier,
)
from pymq.testing import DeterministicRetryScheduler


@pytest.mark.asyncio
async def test_retry_manager_succeeds_and_reports_deterministic_delays() -> None:
    scheduler = DeterministicRetryScheduler()
    attempts = 0

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise OSError("temporary")
        return "ok"

    manager = RetryManager(sleep=scheduler.sleep, clock=scheduler.clock)
    policy = RetryPolicy(max_attempts=4, backoff=FixedBackoff(2), name="publish")
    result = await manager.run(operation, policy, context=RetryContext("publish"))

    assert result == "ok"
    assert attempts == 3
    assert scheduler.delays == [2, 2]


@pytest.mark.asyncio
async def test_terminal_failure_is_not_wrapped_as_exhaustion() -> None:
    async def operation() -> None:
        raise ValueError("invalid application input")

    policy = RetryPolicy(
        classifier=TypeExceptionClassifier(retryable=(OSError,), terminal=(ValueError,))
    )
    with pytest.raises(ValueError, match="invalid application"):
        await RetryManager().run(operation, policy)


@pytest.mark.asyncio
async def test_exhaustion_contains_structured_context_and_cause() -> None:
    async def operation() -> None:
        raise OSError("offline")

    policy = RetryPolicy(max_attempts=2, backoff=FixedBackoff(0), name="network")
    with pytest.raises(RetryExhaustedError) as caught:
        await RetryManager().run(operation, policy, context=RetryContext("connect"))

    assert caught.value.context["attempts"] == 2
    assert caught.value.context["operation"] == "connect"
    assert isinstance(caught.value.__cause__, OSError)


@pytest.mark.asyncio
async def test_retry_budget_stops_a_run_before_attempt_limit() -> None:
    calls = 0

    async def operation() -> None:
        nonlocal calls
        calls += 1
        raise OSError("offline")

    budget = RetryBudget(1)
    policy = RetryPolicy(max_attempts=10, backoff=FixedBackoff(0), budget=budget)
    with pytest.raises(RetryExhaustedError, match="budget"):
        await RetryManager().run(operation, policy)
    assert calls == 2


@pytest.mark.asyncio
async def test_per_attempt_timeout_is_retryable() -> None:
    calls = 0

    async def operation() -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(1)

    policy = RetryPolicy(max_attempts=2, attempt_timeout=0.001, backoff=FixedBackoff(0))
    with pytest.raises(RetryExhaustedError):
        await RetryManager().run(operation, policy)
    assert calls == 2
