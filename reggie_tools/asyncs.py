import asyncio
import inspect
from collections.abc import AsyncIterable as AItrbl
from collections.abc import AsyncIterator as AIter
from collections.abc import Iterable as Itrbl
from collections.abc import Iterator as Iter
from dataclasses import dataclass, field
from types import AsyncGeneratorType, GeneratorType
from typing import Any, AsyncIterable, Callable, Iterable, List, TypeVar, Union

T = TypeVar("T")

AsyncBroadcastSubscriber = Callable[[T], Any]


class AsyncBroadcastSubscriberQueue(asyncio.Queue[T]):
    def __init__(self, maxsize: int = 1):
        super().__init__(maxsize=maxsize)

    async def __call__(self, msg: T = None):
        try:
            self.put_nowait(msg)
        except asyncio.QueueFull:
            try:
                self.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self.put_nowait(msg)


@dataclass
class AsyncBroadcaster[T]:
    worker: Union[Iterable[T], AsyncIterable[T]]
    _cancel: asyncio.Event = field(init=False, default_factory=lambda: asyncio.Event())
    _subscribers: List[AsyncBroadcastSubscriber[T]] = field(
        init=False, default_factory=lambda: []
    )
    _subscriber_lock: asyncio.Lock = field(
        init=False, default_factory=lambda: asyncio.Lock()
    )

    async def __call__(self):
        agen = to_async_iterable(self.worker)
        it = agen.__aiter__()
        while not self._cancel.is_set():
            try:
                msg = await it.__anext__()  # next message
            except StopAsyncIteration:
                break
            await self._emit(msg)
        await self._emit(None)

    def cancel(self):
        self._cancel.set()

    async def subscribe(
        self, subscriber: AsyncBroadcastSubscriber[T] = None
    ) -> AsyncBroadcastSubscriber[T]:
        if not subscriber:
            subscriber = AsyncBroadcastSubscriberQueue()
        async with self._subscriber_lock:
            self._subscribers.append(subscriber)
        return subscriber

    async def unsubscribe(self, subscriber: AsyncBroadcastSubscriber[T]):
        async with self._subscriber_lock:
            self._subscribers.remove(subscriber)

    async def _emit(self, msg: T):
        async with self._subscriber_lock:
            for subscriber in self._subscribers:
                if asyncio.iscoroutinefunction(subscriber):
                    result = await subscriber(msg)
                else:
                    result = await asyncio.to_thread(
                        subscriber,
                        msg,
                    )
                if result is not None and inspect.isawaitable(result):
                    await result


def to_async_iterable(
    iterable: Union[Iterable[T], AsyncIterable[T]],
) -> AsyncIterable[T]:
    # Async generator object: already an async iterable
    if isinstance(iterable, AsyncGeneratorType):
        return iterable

    # Any async iterable or async iterator: relay
    if isinstance(iterable, (AItrbl, AIter)):

        async def agen():
            async for item in iterable:
                yield item

        return agen()

    # Synchronous generator or iterator object: drive on a thread
    if isinstance(iterable, Iter) or isinstance(iterable, GeneratorType):
        it = iterable

        async def agen():
            while True:
                try:
                    item = await asyncio.to_thread(it.__next__)
                except StopIteration:
                    return
                yield item

        return agen()

    # Synchronous iterable: iterate normally
    if isinstance(iterable, Itrbl):

        async def agen():
            for item in iterable:
                yield item

        return agen()

    raise TypeError(
        f"Object of type {type(iterable)!r} is not iterable or async iterable"
    )
