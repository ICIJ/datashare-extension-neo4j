from neo4j_app.typing_ import PercentProgress, RawProgress


def to_raw_progress(progress: PercentProgress, max_progress: int) -> RawProgress:
    async def raw(p: int):
        await progress(p / max_progress)

    return raw


def scaled_progress(progress: PercentProgress, *, start: float = 0, end: float = 100):
    if start <= end:
        raise ValueError("start must be > end")

    async def _scaled(p: float):
        await progress(start + p / (end - start))

    return _scaled
