"""
Simplified implementation of AllenNLP Registrable:
https://github.com/allenai/allennlp
"""

import logging
import os
from abc import ABC
from collections import defaultdict
from typing import (
    Callable,
    ClassVar,
    DefaultDict,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)

from pydantic import BaseSettings, Field

from icij_worker.utils import FromConfig
from icij_worker.utils.from_config import T
from icij_worker.utils.imports import VariableNotFound, import_variable

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_C = TypeVar("_C", bound="RegistrableConfig")
_RegistrableT = TypeVar("_RegistrableT", bound="Registrable")
_SubclassRegistry = Dict[str, _RegistrableT]


class RegistrableMixin(ABC):
    _registry: ClassVar[DefaultDict[type, _SubclassRegistry]] = defaultdict(dict)

    default_implementation: Optional[str] = None

    @classmethod
    def register(
        cls, name: Optional[str] = None, exist_ok: bool = False
    ) -> Callable[[Type[_T]], Type[_T]]:
        # pylint: disable=protected-access
        registry = Registrable._registry[cls]

        def add_subclass_to_registry(subclass: Type[_T]) -> Type[_T]:
            registered_name = name
            if registered_name is None:
                registered_key = subclass.registry_key.default
                if registered_key is None:
                    raise ValueError(
                        "no name provided and the class doesn't define a registry key"
                    )
                registered_name = getattr(subclass, registered_key).default

            if registered_name in registry:
                if exist_ok:
                    msg = (
                        f"{registered_name} has already been registered as "
                        f"{registry[registered_name].__name__}, but exist_ok=True, "
                        f"so overwriting with {cls.__name__}"
                    )
                    logger.info(msg)
                else:
                    msg = (
                        f"Cannot register {registered_name} as {cls.__name__}; "
                        f"name already in use for {registry[registered_name].__name__}"
                    )
                    raise ValueError(msg)
            registry[registered_name] = subclass
            return subclass

        return add_subclass_to_registry

    @classmethod
    def by_name(cls: Type[_RegistrableT], name: str) -> Callable[..., _RegistrableT]:
        logger.debug("instantiating registered subclass %s of %s", name, cls)
        subclass = cls.resolve_class_name(name)
        return cast(Type[_RegistrableT], subclass)

    @classmethod
    def resolve_class_name(cls: Type[_RegistrableT], name: str) -> Type[_RegistrableT]:
        # pylint: disable=protected-access
        if name in Registrable._registry[cls]:
            subclass = Registrable._registry[cls][name]
            return subclass
        if "." in name:
            try:
                subclass = import_variable(name)
            except ModuleNotFoundError as e:
                raise ValueError(
                    f"tried to interpret {name} as a path to a class "
                    f"but unable to import module {'.'.join(name.split('.')[:-1])}"
                ) from e
            except VariableNotFound as e:
                split = name.split(".")
                raise ValueError(
                    f"tried to interpret {name} as a path to a class "
                    f"but unable to find class {split[-1]} in {split[:-1]}"
                ) from e
            return subclass
        available = "\n-".join(cls.list_available())
        msg = f"""{name} is not a registered name for '{cls.__name__}'.
Available names are:
{available}

If your registered class comes from custom code, you'll need to import the\
 corresponding modules and use fully-qualified paths: "my_module.submodule.MyClass"
"""
        raise ValueError(msg)

    @classmethod
    def list_available(cls) -> List[str]:
        # pylint: disable=protected-access
        keys = list(Registrable._registry[cls].keys())
        return keys


class RegistrableConfig(BaseSettings, RegistrableMixin):
    registry_key: ClassVar[str] = Field(const=True, default="name")

    @classmethod
    def from_env(cls: Type[_C]):
        key = cls.registry_key.default
        if cls.__config__.env_prefix is not None:
            key = cls.__config__.env_prefix + key
        registry_key = find_in_env(key, cls.__config__.case_sensitive)
        subcls = cls.resolve_class_name(registry_key)
        return subcls()


class Registrable(RegistrableMixin, FromConfig, ABC):
    @classmethod
    def from_config(cls: Type[T], config: _C, **extras) -> T:
        name = getattr(config, config.registry_key.default).default
        subcls = cls.resolve_class_name(name)
        return subcls._from_config(config, **extras)  # pylint: disable=protected-access


def find_in_env(variable: str, case_sensitive: bool) -> str:
    if case_sensitive:
        try:
            return os.environ[variable]
        except KeyError as e:
            raise ValueError(f"couldn't find {variable} in env variables") from e
    lowercase = variable.lower()
    for k, v in os.environ.items():
        if k.lower() == lowercase:
            return v
    raise ValueError(f"couldn't find {variable.upper()} in env variables")
