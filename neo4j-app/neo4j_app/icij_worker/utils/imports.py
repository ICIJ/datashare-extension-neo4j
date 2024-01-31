import importlib
from typing import Any


class VariableNotFound(ImportError):
    pass


def import_variable(name: str) -> Any:
    parts = name.split(".")
    submodule = ".".join(parts[:-1])
    variable_name = parts[-1]
    try:
        module = importlib.import_module(submodule)
    except ModuleNotFoundError as e:
        raise VariableNotFound(e.msg) from e
    try:
        subclass = getattr(module, variable_name)
    except AttributeError as e:
        raise VariableNotFound(e) from e
    return subclass
