from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import cached_property
from typing import Dict, Generator, Optional, Tuple, Type

from pika import BaseConnection, BasicProperties, DeliveryMode, URLParameters
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.channel import Channel
from pika.exceptions import StreamLostError
from pika.exchange_type import ExchangeType
from pika.spec import Basic
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from neo4j_app.icij_worker.utils import LogWithNameMixin, parse_stream_lost_error

logger = logging.getLogger(__name__)


class MessagePublisher(LogWithNameMixin):
    _logger = logger
    _EXCHANGE_TYPE = ExchangeType.topic

    def __init__(
        self,
        *,
        name: str,
        exchange: str,
        broker_url: str,
        queue: str,
        routing_key: str,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
    ):
        self._name = name
        self._exchange = exchange
        self._queue = queue
        self._broker_url = broker_url
        self._routing_key = routing_key
        self._app_id = app_id

        self._recover_from = recover_from

        self._connection_: Optional[BaseConnection] = None
        self._channel_: Optional[BlockingChannel] = None

    @property
    def logged_name(self) -> str:
        return self._name

    @property
    def _connection(self) -> BlockingConnection:
        if self._connection_ is None:
            msg = (
                f"Publisher has no connection, please call"
                f" {MessagePublisher.connect.__name__}"
            )
            raise ValueError(msg)
        return self._connection_

    @property
    def _channel(self) -> BlockingChannel:
        if self._channel_ is None:
            msg = (
                f"Publisher has no channel, please call"
                f" {MessagePublisher.connect.__name__}"
            )
            raise ValueError(msg)
        return self._channel_

    @cached_property
    def _exception_namespace(self) -> Dict:
        ns = dict(globals())
        ns.update({exc_type.__name__: exc_type for exc_type in self._recover_from})
        return ns

    @contextmanager
    def connect(
        self, max_connection_attempts: int = 1, max_reconnection_wait_s: int = 1.0
    ) -> MessagePublisher:
        try:
            if max_connection_attempts > 1:
                reconnect_wrapper = self.reconnection_attempts(
                    max_connection_attempts, max_reconnection_wait_s
                )
                for attempt in reconnect_wrapper:
                    with attempt:
                        attempt = attempt.attempt_number - 1
                        self._attempt_connect(attempt)
            else:
                self._attempt_connect(0)
                yield self
        finally:
            self.close()

    def _attempt_connect(self, attempt: int):
        if self._connection_ is None or not self._connection.is_open:
            if attempt == 0:
                self._log(logging.INFO, "creating new connection...")
            else:
                self._log(
                    logging.INFO, "recreating closed connection attempt #%s...", attempt
                )
            self._connection_ = BlockingConnection(URLParameters(self._broker_url))
            self._log(logging.INFO, "connection (re)created !")
        self._log(logging.INFO, "reopening channel...")
        self._open_channel()
        self._log(logging.INFO, "channel opened !")

    def reconnection_attempts(
        self, max_attempt: int, max_wait_s: float = 3600.0
    ) -> Generator[RetryCallState, None]:
        for i, attempt in enumerate(self._reconnect_retry(max_attempt, max_wait_s)):
            if i:
                self._attempt_connect(i)
            yield attempt

    def confirm_delivery(self) -> MessagePublisher:
        self._log(logging.INFO, "turning on delivery confirmation")
        self._channel.confirm_delivery()
        return self

    def publish_message(
        self,
        message: bytes,
        *,
        delivery_mode: DeliveryMode,
        mandatory: bool,
        properties: Optional[Dict] = None,
    ):
        self._log(logging.DEBUG, "publishing message...")
        if properties is None:
            properties = dict()
        properties = BasicProperties(
            app_id=self._app_id, delivery_mode=delivery_mode, **properties
        )
        self._channel.basic_publish(
            self._exchange, self._routing_key, message, properties, mandatory=mandatory
        )
        self._log(logging.DEBUG, "message published")

    def close(self):
        if self._connection_ is not None and self._connection.is_open:
            self._log(logging.INFO, "closing connection...")
            self._connection_.close()
            self._log(logging.INFO, "connection closed !")

    def _reconnect_retry(self, max_attempts: int, max_wait_s: float) -> Retrying:
        retry = Retrying(
            wait=wait_exponential(max=max_wait_s),
            stop=stop_after_attempt(max_attempts),
            reraise=True,
            retry=retry_if_exception(self._should_reconnect),
        )
        return retry

    def _open_channel(self):
        self._log(logging.DEBUG, "opening a new channel")
        self._channel_ = self._connection.channel()
        self._channel.add_on_return_callback(self._on_return_callback)
        # TODO: handle qos
        # self._channel.basic_qos(prefetch_count=self._PREFETCH_COUNT)
        self._declare_exchange()
        self._declare_and_bind_queue()

    def _declare_exchange(self):
        self._log(logging.DEBUG, "(re)declaring exchange %s", self._exchange)
        self._channel.exchange_declare(
            exchange=self._exchange, exchange_type=self._EXCHANGE_TYPE, durable=True
        )

    def _declare_and_bind_queue(self):
        self._log(logging.DEBUG, "(re)declaring queue %s", self._queue)
        self._channel.queue_declare(self._queue, durable=True)
        self._channel.queue_bind(self._queue, self._exchange, self._routing_key)

    def _on_return_callback(
        self,
        _channel: Channel,
        _method: Basic.Return,
        properties: BasicProperties,
        body: bytes,
    ):
        # pylint: disable=invalid-name
        self._log(
            logging.ERROR,
            "published message was rejected by the broker."
            "\nProperties: %r"
            "\nBody: %.1000s",
            properties,
            body,
        )

    def _should_reconnect(self, exception: BaseException) -> bool:
        if isinstance(exception, StreamLostError):
            exception = parse_stream_lost_error(
                exception, namespace=self._exception_namespace
            )
        return isinstance(exception, self._recover_from)

    def _on_disconnect_callback(self, retry_state: RetryCallState):
        exception = retry_state.outcome.exception()
        exception = f"({exception.__class__.__name__} {exception})"
        self._log(
            logging.ERROR,
            "recoverable exception occurred, trying to reconnect, attempt %s: %s",
            retry_state.attempt_number,
            exception,
        )
        self._attempt_connect(retry_state.attempt_number)
