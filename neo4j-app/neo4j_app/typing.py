from typing import Awaitable, Callable

PercentProgress = Callable[[float], Awaitable[None]]
RawProgress = Callable[[int], Awaitable[None]]
