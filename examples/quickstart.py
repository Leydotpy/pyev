"""Executable pyev local-engine quick start."""

import asyncio
from dataclasses import dataclass

from pyev import Broker, Delivery, event


@event("example.greeting.created", version=1)
@dataclass(frozen=True, slots=True)
class GreetingCreated:
    name: str


async def main() -> None:
    async def greet(delivery: Delivery[GreetingCreated]) -> None:
        print(f"Hello, {delivery.message.name}!")

    async with Broker.from_config({"engine": "local"}) as broker:
        await broker.subscribe("example.greeting.*", greet)
        await broker.publish(GreetingCreated("Ada"))


if __name__ == "__main__":
    asyncio.run(main())
