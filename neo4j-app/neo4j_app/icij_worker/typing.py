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


class ConsumerProtocol(Protocol):
    def acknowledge_message(self, delivery_tag: int):
        ...

    def reject_message(self, delivery_tag: int, requeue: bool):
        ...

    def log(self, level: int, *args, **kwargs):
        ...
