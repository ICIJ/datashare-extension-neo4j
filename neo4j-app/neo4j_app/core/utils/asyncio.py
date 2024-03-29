import asyncio
from typing import (
    AsyncGenerator,
    AsyncIterable,
    Coroutine,
    Iterable,
    Sequence,
    TypeVar,
    Union,
)

from aiostream.stream import flatten

T = TypeVar("T")


async def run_with_concurrency(
    aws: Iterable[Union[Coroutine, asyncio.Future]], max_concurrency: int
) -> AsyncGenerator:
    max_concurrency = asyncio.Semaphore(max_concurrency)
    aws = [_run_with_semaphore(aw, max_concurrency) for aw in aws]
    for res in asyncio.as_completed(aws):
        yield await res


async def _to_async(it: Iterable[T]) -> AsyncIterable[T]:
    for item in it:
        yield item


def iterate_with_concurrency(
    iterables: Sequence[AsyncIterable[T]], max_concurrency: int
):
    if not iterables:
        raise ValueError()
    streamer = flatten(_to_async(iterables), task_limit=max_concurrency)
    return streamer


async def _run_with_semaphore(
    aws: Union[Coroutine, asyncio.Future], sem: asyncio.Semaphore
):
    async with sem:
        return await aws
