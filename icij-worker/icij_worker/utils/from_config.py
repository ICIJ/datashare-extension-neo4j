from abc import ABC, abstractmethod
from functools import cached_property
from typing import Type, TypeVar

from pydantic import BaseSettings

T = TypeVar("T", bound="FromConfig")
C = TypeVar("C", bound=BaseSettings)


class FromConfig(ABC):
    @classmethod
    @abstractmethod
    def _from_config(cls: Type[T], config: C, **extras) -> T: ...

    @abstractmethod
    def _to_config(self) -> C: ...

    @cached_property
    def config(self) -> C:
        return self._to_config()
