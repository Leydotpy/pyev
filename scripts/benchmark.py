"""Reproducible local/memory publish-throughput baseline for pyev."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import time
from collections.abc import Sequence

from pymq import Broker, Delivery


async def _run_once(engine: str, messages: int, payload_bytes: int) -> float:
    received = 0
    completed = asyncio.Event()
    payload = "x" * payload_bytes

    async def handler(_delivery: Delivery[object]) -> None:
        nonlocal received
        received += 1
        if received == messages:
            completed.set()

    async with Broker.from_config({"engine": engine}) as broker:
        await broker.subscribe("benchmark.message", handler)
        started = time.perf_counter()
        for sequence in range(messages):
            await broker.publish(
                {"sequence": sequence, "payload": payload},
                route="benchmark.message",
            )
        async with asyncio.timeout(60):
            await completed.wait()
        elapsed = time.perf_counter() - started
    return messages / elapsed


async def _benchmark(engine: str, messages: int, payload_bytes: int, rounds: int) -> None:
    samples = [await _run_once(engine, messages, payload_bytes) for _round in range(rounds)]
    print(
        json.dumps(
            {
                "engine": engine,
                "messages_per_round": messages,
                "payload_bytes": payload_bytes,
                "rounds": rounds,
                "median_messages_per_second": round(statistics.median(samples), 2),
                "min_messages_per_second": round(min(samples), 2),
                "max_messages_per_second": round(max(samples), 2),
                "python": platform.python_version(),
                "platform": platform.platform(),
            },
            indent=2,
            sort_keys=True,
        )
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("local", "memory"), default="memory")
    parser.add_argument("--messages", type=int, default=10_000)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--rounds", type=int, default=3)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    arguments = _parser().parse_args(argv)
    if arguments.messages < 1 or arguments.payload_bytes < 0 or arguments.rounds < 1:
        raise SystemExit("messages/rounds must be positive and payload-bytes non-negative")
    asyncio.run(
        _benchmark(
            arguments.engine,
            arguments.messages,
            arguments.payload_bytes,
            arguments.rounds,
        )
    )


if __name__ == "__main__":
    main()
