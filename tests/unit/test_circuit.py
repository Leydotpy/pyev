from __future__ import annotations

import pytest

from broka.exceptions import CircuitOpenError
from broka.reliability import CircuitBreaker, CircuitBreakerConfig, CircuitState
from broka.testing import DeterministicClock


@pytest.mark.asyncio
async def test_circuit_opens_half_opens_and_recovers() -> None:
    clock = DeterministicClock()
    breaker = CircuitBreaker(
        "redis",
        CircuitBreakerConfig(failure_threshold=2, recovery_timeout=5),
        clock=clock,
    )

    await breaker.record_failure(OSError("one"))
    await breaker.record_failure(OSError("two"))
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        await breaker.before_call()

    clock.advance(5)
    await breaker.before_call()
    assert breaker.state is CircuitState.HALF_OPEN
    await breaker.record_success()
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_failure_rate_window_can_open_circuit() -> None:
    breaker = CircuitBreaker(
        "kafka",
        CircuitBreakerConfig(
            failure_threshold=100,
            failure_rate_threshold=0.5,
            minimum_calls=4,
            window_size=4,
        ),
    )
    await breaker.record_success()
    await breaker.record_success()
    await breaker.record_failure()
    await breaker.record_failure()
    assert breaker.state is CircuitState.OPEN


@pytest.mark.asyncio
async def test_half_open_probe_limit_is_enforced() -> None:
    clock = DeterministicClock()
    breaker = CircuitBreaker(
        "amqp",
        CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0, half_open_max_calls=1),
        clock=clock,
    )
    await breaker.record_failure()
    await breaker.before_call()
    with pytest.raises(CircuitOpenError):
        await breaker.before_call()
