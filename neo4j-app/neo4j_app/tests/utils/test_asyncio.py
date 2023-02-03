import asyncio

import pytest

from neo4j_app.utils.asyncio import run_concurrently


@pytest.mark.asyncio
async def test_run_concurrently():
    # Given
    short = 0.001
    long = 0.1
    sleeps = [long, long, short, short, short, short]

    async def _sleep(t: float) -> float:
        await asyncio.sleep(t)
        return t

    tasks = (_sleep(t) for t in sleeps)
    max_concurrency = 3

    # When
    res = [r async for r in run_concurrently(tasks, max_concurrency=max_concurrency)]

    # Then
    expected_res = [short, short, short, short, long, long]
    assert res == expected_res
