from collections import namedtuple
from typing import Awaitable, Callable

PercentProgress = Callable[[float], Awaitable[None]]
RawProgress = Callable[[int], Awaitable[None]]
LightCounters = namedtuple("LightCounters", ["nodes_created", "relationships_created"])
