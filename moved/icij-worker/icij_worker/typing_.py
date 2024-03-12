from types import TracebackType
from typing import (
    Awaitable,
    Callable,
    Coroutine,
    Optional,
    Protocol,
    Tuple,
    Type,
    Union,
)

DependencyLabel = Optional[str]
DependencySetup = Callable[..., None]
DependencyAsyncSetup = Callable[..., Coroutine[None, None, None]]

PercentProgress = Callable[[float], Awaitable[None]]
RawProgress = Callable[[int], Awaitable[None]]


class DependencyTeardown(Protocol):
    def __call__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[TracebackType],
    ) -> None: ...


class DependencyAsyncTeardown(Protocol):
    async def __call__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[TracebackType],
    ) -> None: ...


Dependency = Tuple[
    DependencyLabel,
    Union[DependencySetup, DependencyAsyncSetup],
    Optional[Union[DependencyTeardown, DependencyAsyncTeardown]],
]
