import inspect
import logging
import sys
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, List

from icij_worker.typing_ import Dependency

logger = logging.getLogger(__name__)


class DependencyInjectionError(RuntimeError):
    def __init__(self, name: str):
        msg = f"{name} was not injected"
        super().__init__(msg)


@contextmanager
def _log_exception_and_continue():
    try:
        yield
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Exception %s occurred ", exc, exc_info=True)


@asynccontextmanager
async def run_deps(
    dependencies: List[Dependency], ctx: str, **kwargs
) -> AsyncGenerator[None, None]:
    to_close = []
    original_ex = None
    try:
        logger.info("Setting up dependencies for %s...", ctx)
        for name, enter_fn, exit_fn in dependencies:
            if enter_fn is not None:
                if name is not None:
                    logger.debug("applying: %s", name)
                if inspect.iscoroutinefunction(enter_fn):
                    await enter_fn(**kwargs)
                else:
                    enter_fn(**kwargs)
            to_close.append((name, exit_fn))
        yield
    except Exception as e:  # pylint: disable=broad-exception-caught
        original_ex = e
        logger.error(
            "Exception occurred while opening dependency: %s", e, exc_info=True
        )
    finally:
        to_raise = []
        if original_ex is not None:
            to_raise.append(original_ex)
        logger.info("Rolling back dependencies for %s...", ctx)
        for name, exit_fn in to_close[::-1]:
            if exit_fn is None:
                continue
            try:
                if name is not None:
                    logger.debug("rolling back %s", name)
                exc_info = sys.exc_info()
                with _log_exception_and_continue():
                    if inspect.iscoroutinefunction(exit_fn):
                        await exit_fn(*exc_info)
                    else:
                        exit_fn(*exc_info)
            except Exception as e:  # pylint: disable=broad-exception-caught
                to_raise.append(e)
        logger.debug("Rolled back all dependencies for %s !", ctx)
        if to_raise:
            for e in to_raise:
                logger.error("Error while handling dependencies %s!", e)
            raise RuntimeError(to_raise)
