from copy import copy
from functools import cached_property

from pydantic import BaseModel


def to_lower_camel(field: str) -> str:
    return "".join(
        w.capitalize() if i > 0 else w for i, w in enumerate(field.split("_"))
    )


_FIELD_ARGS = ["include", "exclude", "update"]


# TODO: remove this one when migrating to pydantic 2.0
def safe_copy(model: BaseModel, **kwargs) -> BaseModel:
    for k in _FIELD_ARGS:
        for field in kwargs[k]:
            if not hasattr(model, field):
                msg = f"Unknown attribute {field} for {model.__class__}"
                raise AttributeError(msg)

    return model.copy(**kwargs)


class BaseICIJModel(BaseModel):
    class Config:
        allow_mutation = False
        extra = "forbid"
        allow_population_by_field_name = True
        keep_untouched = (cached_property,)
        use_enum_values = True

    def dict(self, **kwargs):
        kwargs = copy(kwargs)
        if "by_alias" in kwargs:
            by_alias = kwargs.pop("by_alias")
            if not by_alias:
                raise f"Can't serialize a {BaseICIJModel} without using alias"
        return super().dict(by_alias=True, **kwargs)


class LowerCamelCaseModel(BaseICIJModel):
    class Config:
        alias_generator = to_lower_camel


class IgnoreExtraModel(BaseICIJModel):
    class Config:
        extra = "ignore"


class NoEnumModel(BaseICIJModel):
    class Config:
        use_enum_values = False
