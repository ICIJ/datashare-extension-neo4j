from copy import copy
from functools import cached_property

from pydantic import BaseModel


def to_lower_camel(field: str) -> str:
    return "".join(
        w.capitalize() if i > 0 else w for i, w in enumerate(field.split("_"))
    )


class BaseICIJModel(BaseModel):
    class Config:
        allow_mutation = False
        extra = "forbid"
        allow_population_by_field_name = True
        keep_untouched = (cached_property,)

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
