from datetime import datetime
from typing import Awaitable

from neo4j_app.typing_ import PercentProgress, RawProgress


class CheckCancelledProgress:
    def __init__(
        self,
        task_id: str,
        progress: PercentProgress,
        *,
        check_cancelled: [[str], Awaitable],
        refresh_cancelled: [[], Awaitable],
        refresh_interval_s: float,
    ):
        self._task_id = task_id
        self._progress = progress
        self._check_cancelled = check_cancelled
        self._refresh_cancelled = refresh_cancelled
        self._refresh_interval_s = refresh_interval_s
        self._last_check = None

    async def __call__(self, progress: float):
        elapsed = None
        if self._last_check:
            elapsed = (datetime.now() - self._last_check).total_seconds()
        if not self._last_check or elapsed > self._refresh_interval_s:
            await self._refresh_cancelled()
            self._last_check = datetime.now()
        await self._check_cancelled(task_id=self._task_id)
        await self._progress(progress)


def to_raw_progress(progress: PercentProgress, max_progress: int) -> RawProgress:
    if not max_progress > 0:
        raise ValueError("max_progress must be > 0")

    async def raw(p: int):
        await progress(p / max_progress * 100)

    return raw


def to_scaled_progress(
    progress: PercentProgress, *, start: float = 0.0, end: float = 100.0
):
    if not 0 <= start < end:
        raise ValueError("start must be [0, end[")
    if not start < end <= 100:
        raise ValueError("end must be ]start, 100]")

    async def _scaled(p: float):
        await progress(start + p * (end - start) / 100)

    return _scaled
