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


class LowerCamelCaseModel(BaseICIJModel):
    class Config:
        alias_generator = to_lower_camel


class IgnoreExtraModel(BaseICIJModel):
    class Config:
        extra = "ignore"
