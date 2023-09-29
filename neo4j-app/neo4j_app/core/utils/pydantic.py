from datetime import datetime
from functools import cached_property
from typing import cast

from pydantic import BaseModel


def to_lower_camel(field: str) -> str:
    return "".join(
        w.capitalize() if i > 0 else w for i, w in enumerate(field.split("_"))
    )


_FIELD_ARGS = ["include", "exclude", "update"]

_SCHEMAS = dict()


# TODO: remove this one when migrating to pydantic 2.0
def safe_copy(model: BaseModel, **kwargs):
    if model.__class__ not in _SCHEMAS:
        _SCHEMAS[model.__class__] = dict()
        # Model.copy is always without alias
        _SCHEMAS[model.__class__] = model.__class__.schema(by_alias=False)
    schema = _SCHEMAS[model.__class__]
    for k in _FIELD_ARGS:
        if not k in kwargs:
            continue
        for field in kwargs[k]:
            if not field in schema["properties"]:
                msg = f'Unknown attribute "{field}" for {model.__class__}'
                raise AttributeError(msg)

    return cast(model.__class__, model.copy(**kwargs))


class BaseICIJModel(BaseModel):
    class Config:
        allow_mutation = False
        extra = "forbid"
        allow_population_by_field_name = True
        keep_untouched = (cached_property,)
        use_enum_values = True


class LowerCamelCaseModel(BaseICIJModel):
    class Config:
        alias_generator = to_lower_camel


class IgnoreExtraModel(BaseICIJModel):
    class Config:
        extra = "ignore"


class NoEnumModel(BaseICIJModel):
    class Config:
        use_enum_values = False


class ISODatetime(BaseICIJModel):
    class Config:
        json_encoders = {datetime: lambda x: x.isoformat()}
