import inspect
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, cast

import neo4j
from fastapi import FastAPI

from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClientABC


class DependencyInjectionError(RuntimeError):
    def __init__(self, name: str):
        msg = f"{name} was not injected"
        super().__init__(msg)


_NEO4J_DRIVER: Optional[neo4j.AsyncDriver] = None
_ES_CLIENT: Optional[ESClientABC] = None


def get_global_config_dep() -> AppConfig:
    # TODO: not sure this is still necessary now that we are using lifespans
    return AppConfig.get_global_config()


async def neo4j_driver_enter(app: FastAPI):
    global _NEO4J_DRIVER
    _NEO4J_DRIVER = app.state.config.to_neo4j_driver()
    await _NEO4J_DRIVER.__aenter__()  # pylint: disable=unnecessary-dunder-call


async def neo4j_driver_exit(exc_type, exc_value, traceback):
    await _NEO4J_DRIVER.__aexit__(exc_type, exc_value, traceback)


def lifespan_neo4j_driver() -> neo4j.AsyncDriver:
    if _NEO4J_DRIVER is None:
        raise DependencyInjectionError("neo4j driver")
    return cast(neo4j.AsyncDriver, _NEO4J_DRIVER)


async def es_client_enter(app: FastAPI):
    global _ES_CLIENT
    _ES_CLIENT = app.state.config.to_es_client()
    await _ES_CLIENT.__aenter__()  # pylint: disable=unnecessary-dunder-call


async def es_client_exit(exc_type, exc_value, traceback):
    await _ES_CLIENT.__aexit__(exc_type, exc_value, traceback)


def lifespan_es_client() -> ESClientABC:
    if _ES_CLIENT is None:
        raise DependencyInjectionError("es client")
    return cast(ESClientABC, _ES_CLIENT)


_LIFESPAN_DEPS = [
    (neo4j_driver_enter, neo4j_driver_exit),
    (es_client_enter, es_client_exit),
]


@asynccontextmanager
async def lifespan_deps(app: FastAPI) -> AsyncGenerator[None, None]:
    to_close = []
    try:
        for enter_fn, exit_fn in _LIFESPAN_DEPS:
            if inspect.iscoroutinefunction(enter_fn):
                await enter_fn(app)
            else:
                enter_fn(app)
            to_close.append(exit_fn)
        yield
    finally:
        to_raise = []
        for exit_fn in to_close:
            try:
                exc_info = sys.exc_info()
                if inspect.iscoroutinefunction(exit_fn):
                    await exit_fn(*exc_info)
                else:
                    exit_fn(*exc_info)
            except Exception as e:  # pylint: disable=broad-exception-caught
                to_raise.append(e)
        if to_raise:
            raise RuntimeError(to_raise)
