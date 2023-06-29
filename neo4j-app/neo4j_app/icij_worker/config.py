from pika.exchange_type import ExchangeType

from neo4j_app.core.utils.pydantic import (
    LowerCamelCaseModel,
    NoEnumModel,
)


class Exchange(NoEnumModel, LowerCamelCaseModel):
    name: str
    type: ExchangeType


class Routing(LowerCamelCaseModel):
    exchange: Exchange
    routing_key: str
    default_queue: str
