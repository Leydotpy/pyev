from __future__ import annotations

import asyncio

import pytest

from pyev.exceptions import LifecycleError
from pyev.lifecycle import (
    LifecycleManager,
    LifecycleState,
    RestartPolicy,
    SupervisedTaskState,
    TaskSupervisor,
)
from pyev.reliability import FixedBackoff


@pytest.mark.asyncio
async def test_task_supervisor_restarts_from_a_fresh_factory() -> None:
    calls = 0

    async def worker() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient")

    supervisor = TaskSupervisor()
    supervisor.start_soon(
        worker,
        name="worker",
        restart_policy=RestartPolicy.ON_FAILURE,
        max_restarts=1,
        backoff=FixedBackoff(0),
    )
    snapshot = await supervisor.wait("worker")

    assert calls == 2
    assert snapshot.state is SupervisedTaskState.COMPLETED
    assert snapshot.restart_count == 1
    assert len(supervisor.failures) == 1


@pytest.mark.asyncio
async def test_task_supervisor_shutdown_leaves_no_owned_tasks() -> None:
    started = asyncio.Event()

    async def worker() -> None:
        started.set()
        await asyncio.Event().wait()

    supervisor = TaskSupervisor()
    supervisor.start_soon(worker, name="long-lived")
    await started.wait()
    await supervisor.shutdown()

    assert supervisor.active_count == 0
    assert supervisor.snapshot("long-lived").state is SupervisedTaskState.CANCELLED


class Component:
    def __init__(self, name: str, log: list[str], *, fail_start: bool = False) -> None:
        self.name = name
        self.log = log
        self.fail_start = fail_start

    async def startup(self) -> None:
        self.log.append(f"start:{self.name}")
        if self.fail_start:
            raise RuntimeError("boom")

    async def shutdown(self) -> None:
        self.log.append(f"stop:{self.name}")


@pytest.mark.asyncio
async def test_lifecycle_startup_is_transactional() -> None:
    log: list[str] = []
    manager = LifecycleManager()
    manager.register("first", Component("first", log))
    manager.register("second", Component("second", log, fail_start=True))

    with pytest.raises(LifecycleError, match="startup"):
        await manager.startup()

    assert manager.state is LifecycleState.FAILED
    assert log == ["start:first", "start:second", "stop:first"]
