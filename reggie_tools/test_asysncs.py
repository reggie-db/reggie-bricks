import asyncio
from collections.abc import (
    AsyncIterator as AIter,
)

from reggie_tools.asyncs import AsyncBroadcaster, to_async_iterable


async def _main():
    async def print_iterable(name: str, input):
        async for i in to_async_iterable(input):
            print(f"{name}: {i}")

    await print_iterable("to_async_iterable sync", [1, 2, 3])

    class Counter(AIter):
        def __init__(self, n: int):
            self.i = 0
            self.n = n

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.i >= self.n:
                raise StopAsyncIteration
            v = self.i
            self.i += 1
            return v

    await print_iterable("to_async_iterable Counter", Counter(5))

    async def worker():
        for i in range(10):
            if i > 0:
                await asyncio.sleep(1)
            if i == 5 and False:
                raise Exception("test")
            print(f"emitting: {i}")
            yield i

    await print_iterable("to_async_iterable async", [1, 2, 3])

    broadcaster = AsyncBroadcaster[int](worker())
    tasks = []

    for i in range(10):
        subscriber_id = f"subscriber_{i}"

        def on_message(msg: int, subscriber_id=subscriber_id):
            print(f"subscriber_id:{subscriber_id} received: {msg}")

        await broadcaster.subscribe(on_message)

        if i == 5:

            async def remove(subscriber=on_message, subscriber_id=subscriber_id):
                await asyncio.sleep(3)
                await broadcaster.unsubscribe(subscriber)
                print(f"subscriber_id:{subscriber_id} removed")

            tasks.append(asyncio.create_task(remove()))
    queue = await broadcaster.subscribe()

    async def queue_read():
        await asyncio.sleep(4)
        print(f"queue_read: {await queue.get()}")

    tasks.append(asyncio.create_task(queue_read()))
    tasks.append(asyncio.create_task(broadcaster()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(_main())
