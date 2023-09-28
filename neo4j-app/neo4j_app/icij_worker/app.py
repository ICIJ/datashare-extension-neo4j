import functools
from functools import cached_property
from typing import Callable, Dict, Optional, Tuple, Type

from pydantic import Field

from neo4j_app.core.config import AppConfig
from neo4j_app.core.utils.pydantic import BaseICIJModel


class RegisteredTask(BaseICIJModel):
    task: Callable
    recover_from: Tuple[Type[Exception], ...] = tuple()
    # TODO: enable max retries
    max_retries: Optional[int] = Field(const=True, default=None)


class ICIJApp:
    def __init__(self, name: str, config: Optional[AppConfig] = None):
        self._name = name
        self._config = config
        self._registry = dict()

    @cached_property
    def registry(self) -> Dict[str, RegisteredTask]:
        return self._registry

    @property
    def config(self) -> Optional[AppConfig]:
        return self._config

    @config.setter
    def config(self, value: AppConfig):
        self._config = value

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
