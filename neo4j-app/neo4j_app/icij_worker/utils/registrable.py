"""
Simplified implementation of AllenNLP Registrable:
https://github.com/allenai/allennlp
"""
import logging
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

from neo4j_app.icij_worker.utils.from_config import FromConfig, T
from neo4j_app.icij_worker.utils.imports import VariableNotFound, import_variable

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_C = TypeVar("_C", bound="RegistrableConfig")
_RegistrableT = TypeVar("_RegistrableT", bound="Registrable")
_SubclassRegistry = Dict[str, _RegistrableT]


class RegistrableConfig(BaseSettings):
    registry_key: str = Field(const=True, default="name")


class Registrable(FromConfig, ABC):
    _registry: ClassVar[DefaultDict[type, _SubclassRegistry]] = defaultdict(dict)

    default_implementation: Optional[str] = None

    @classmethod
    def register(
        cls, name: str, exist_ok: bool = False
    ) -> Callable[[Type[_T]], Type[_T]]:
        registry = Registrable._registry[cls]

        def add_subclass_to_registry(subclass: Type[_T]) -> Type[_T]:
            if name in registry:
                if exist_ok:
                    msg = (
                        f"{name} has already been registered as "
                        f"{registry[name].__name__}, but exist_ok=True, "
                        f"so overwriting with {cls.__name__}"
                    )
                    logger.info(msg)
                else:
                    msg = (
                        f"Cannot register {name} as {cls.__name__}; "
                        f"name already in use for {registry[name].__name__}"
                    )
                    raise ValueError(msg)
            registry[name] = subclass
            return subclass

        return add_subclass_to_registry

    @classmethod
    def by_name(cls: Type[_RegistrableT], name: str) -> Callable[..., _RegistrableT]:
        logger.debug("instantiating registered subclass %s of %s", name, cls)
        subclass = cls.resolve_class_name(name)
        return cast(Type[_RegistrableT], subclass)

    @classmethod
    def resolve_class_name(cls: Type[_RegistrableT], name: str) -> Type[_RegistrableT]:
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
        msg = f"""{name}' is not a registered name for '{cls.__name__}'.
Available names are:
{available}

If your registered class comes from custom code, you'll need to import the\
 corresponding modules and use fully-qualified paths: "my_module.submodule.MyClass"
"""
        raise ValueError(msg)

    @classmethod
    def list_available(cls) -> List[str]:
        keys = list(Registrable._registry[cls].keys())
        return keys

    @classmethod
    def from_config(cls: Type[T], config: _C, **extras) -> T:
        subcls = cls.resolve_class_name(getattr(config, config.registry_key))
        return subcls._from_config(config, **extras)  # pylint: disable=protected-access
