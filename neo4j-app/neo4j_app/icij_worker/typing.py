from typing import Callable, Protocol

from pika.spec import Basic, BasicProperties

ProgressHandler = Callable[[float], None]


class OnMessage(Protocol):
    def __call__(
        self,
        consumer,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        ...
