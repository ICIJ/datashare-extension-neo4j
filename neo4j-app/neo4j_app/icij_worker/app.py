from __future__ import annotations
import functools
import importlib
from contextlib import asynccontextmanager
from functools import cached_property
from typing import Callable, Dict, List, Optional, Tuple, Type, final

from pydantic import Field

from neo4j_app.core.utils.pydantic import BaseICIJModel
from neo4j_app.icij_worker.exceptions import UnknownApp
from neo4j_app.icij_worker.typing_ import Dependency
from neo4j_app.icij_worker.utils.dependencies import run_deps


class RegisteredTask(BaseICIJModel):
    task: Callable
    recover_from: Tuple[Type[Exception], ...] = tuple()
    # TODO: enable max retries
    max_retries: Optional[int] = Field(const=True, default=None)


class AsyncApp:
    def __init__(self, name: str, dependencies: Optional[List[Dependency]] = None):
        self._name = name
        self._registry = dict()
        if dependencies is None:
            dependencies = []
        self._dependencies = dependencies

    @cached_property
    def registry(self) -> Dict[str, RegisteredTask]:
        return self._registry

    @property
    def name(self) -> str:
        return self._name

    def task(
        self,
        name: Optional[str] = None,
        recover_from: Tuple[Type[Exception]] = tuple(),
        max_retries: Optional[int] = None,
    ) -> Callable:
        if callable(name) and not recover_from and max_retries is None:
            f = name
            return functools.partial(self._register_task, name=f.__name__)(f)
        return functools.partial(
            self._register_task,
            name=name,
            recover_from=recover_from,
            max_retries=max_retries,
        )

    @final
    @asynccontextmanager
    async def lifetime_dependencies(self, **kwargs):
        ctx = f"{self.name} async app"
        async with run_deps(self._dependencies, ctx=ctx, **kwargs):
            yield

    def _register_task(
        self,
        f: Callable,
        *,
        name: Optional[str] = None,
        recover_from: Tuple[Type[Exception]] = tuple(),
        max_retries: Optional[int] = None,
    ) -> Callable:
        if name is None:
            name = f.__name__
        registered = self._registry.get(name)
        if registered is not None:
            raise ValueError(f'Task "{name}" is already registered: {registered}')
        self._registry[name] = RegisteredTask(
            task=f, max_retries=max_retries, recover_from=recover_from
        )

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    @classmethod
    def load(cls, app_path: str) -> AsyncApp:
        app_path = app_path.split(".")
        module, app_name = app_path[:-1], app_path[-1]
        module = ".".join(module)
        try:
            module = importlib.import_module(module)
        except ModuleNotFoundError as e:
            msg = f'Expected app_path to be the fully qualified path to a \
            {AsyncApp.__name__} instance "my_module.my_app_instance"'
            raise UnknownApp(msg) from e
        app = getattr(module, app_name)
        return app
