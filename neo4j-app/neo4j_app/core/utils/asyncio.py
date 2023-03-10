import asyncio
from typing import AsyncGenerator, Coroutine, Iterable, Union


async def run_with_concurrency(
    aws: Iterable[Union[Coroutine, asyncio.Future]], max_concurrency: int
) -> AsyncGenerator:
    max_concurrency = asyncio.Semaphore(max_concurrency)
    aws = [_run_with_semaphore(aw, max_concurrency) for aw in aws]
    for res in asyncio.as_completed(aws):
        yield await res


async def _run_with_semaphore(
    aws: Union[Coroutine, asyncio.Future], sem: asyncio.Semaphore
):
    async with sem:
        return await aws
