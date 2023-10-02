from typing import List

import pytest

from neo4j_app.core.utils.progress import to_raw_progress, to_scaled_progress


class MockProgress:
    def __init__(self):
        self._progress = []

    @property
    def progress(self) -> List[float]:
        return self._progress

    async def __call__(self, p: float):
        self._progress.append(p)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "max_progress,raw,expected_progress",
    [
        (20, [2, 10, 15, 20], [10.0, 50.0, 75.0, 100.0]),
        (5, [1, 2, 3, 4, 5], [20.0, 40.0, 60.0, 80.0, 100.0]),
    ],
)
async def test_to_raw_progress(
    max_progress: int, raw: List[int], expected_progress: List[float]
):
    # Given
    progress = MockProgress()
    raw_progress = to_raw_progress(progress.__call__, max_progress=max_progress)

    # When
    for p in raw:
        await raw_progress(p)

    # Then
    assert progress.progress == expected_progress


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start,end,expected_progress",
    [
        (0.0, 100.0, [0.0, 20.0, 40.0, 60.0, 80.0, 100.0]),
        (0.0, 50.0, [0.0, 10.0, 20.0, 30.0, 40.0, 50.0]),
        (90.0, 100.0, [90.0, 92.0, 94.0, 96.0, 98.0, 100.0]),
    ],
)
async def test_to_scaled_progress(
    start: float, end: float, expected_progress: List[float]
):
    # Given
    progress = MockProgress()
    scaled = to_scaled_progress(progress.__call__, start=start, end=end)

    # When
    for p in range(0, 120, 20):
        await scaled(p)

    # Then
    assert progress.progress == expected_progress


@pytest.mark.asyncio
async def test_to_scaled_and_raw():
    # Given
    progress = MockProgress()
    scaled = to_scaled_progress(progress.__call__, start=50, end=100)
    raw = to_raw_progress(scaled, max_progress=10)
    p_raw = [0, 5, 10]

    # When
    for p in p_raw:
        await raw(p)

    # Then
    expected_progress = [50.0, 75.0, 100.0]
    assert progress.progress == expected_progress
