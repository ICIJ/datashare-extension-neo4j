from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import cached_property
from typing import Dict, Generator, Optional, Tuple, Type

from pika import BaseConnection, BasicProperties, DeliveryMode, URLParameters
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.channel import Channel
from pika.exceptions import StreamLostError
from pika.spec import Basic
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from neo4j_app.icij_worker.config import Routing
from neo4j_app.icij_worker.task import TaskError, TaskEvent, TaskResult
from neo4j_app.icij_worker.utils import LogWithNameMixin, parse_stream_lost_error

logger = logging.getLogger(__name__)


class MessagePublisher(LogWithNameMixin):
    _logger = logger

    def __init__(
        self,
        *,
        name: str,
        event_routing: Routing,
        result_routing: Routing,
        error_routing: Routing,
        broker_url: str,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
    ):
        self._name = name
        self._app_id = app_id
        self._broker_url = broker_url

        self._event_config = event_routing
        self._result_config = result_routing
        self._error_config = error_routing

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
            if self._connection_ is not None and self._connection.is_open:
                self._log(logging.INFO, "closing connection...")
                self._connection_.close()  # This will close the channel too
                self._log(logging.INFO, "connection closed !")

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

    def _publish_message(
        self,
        message: bytes,
        *,
        exchange: str,
        routing_key: Optional[str],
        delivery_mode: DeliveryMode,
        mandatory: bool,
        properties: Optional[Dict] = None,
    ):
        if properties is None:
            properties = dict()
        properties = BasicProperties(
            app_id=self._app_id, delivery_mode=delivery_mode, **properties
        )
        self._channel.basic_publish(
            exchange,
            routing_key,
            message,
            properties,
            mandatory=mandatory,
        )

    def publish_task_event(
        self,
        event: TaskEvent,
        *,
        delivery_mode: DeliveryMode = DeliveryMode.Persistent,
        mandatory: bool,
        properties: Optional[Dict] = None,
    ):
        self._log(logging.DEBUG, "publishing task event %s...", event)
        message = event.json().encode()
        self._publish_message(
            message,
            exchange=self._event_config.exchange.name,
            routing_key=self._event_config.routing_key,
            properties=properties,
            delivery_mode=delivery_mode,
            mandatory=mandatory,
        )
        self._log(logging.DEBUG, "event published for task %s!", event.task_id)

    def publish_task_result(
        self,
        result: TaskResult,
        *,
        properties: Optional[Dict] = None,
    ):
        self._log(logging.DEBUG, "publishing result for task %s...", result.task_id)
        message = result.json().encode()
        self._publish_message(
            message,
            exchange=self._result_config.exchange.name,
            routing_key=self._result_config.routing_key,
            properties=properties,
            delivery_mode=DeliveryMode.Persistent,
            mandatory=True,
        )
        self._log(logging.DEBUG, "result published for task %s!", result.task_id)

    def publish_task_error(
        self,
        error: TaskError,
        *,
        properties: Optional[Dict] = None,
    ):
        self._log(logging.DEBUG, "publishing error for task %s...", error.task_id)
        message = error.json().encode()
        self._publish_message(
            message,
            exchange=self._error_config.exchange.name,
            routing_key=self._error_config.routing_key,
            properties=properties,
            delivery_mode=DeliveryMode.Persistent,
            mandatory=True,
        )
        self._log(logging.DEBUG, "error published for task %s!", error.task_id)

    def close(self):
        if self._connection_ is not None and self._connection.is_open:
            self._connection_.close()

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
        # TODO: handle qos, careful since QOS is per channel, this will affect both
        #  results + events + errors
        # self._channel.basic_qos(prefetch_count=self._PREFETCH_COUNT)
        self._declare_exchanges()
        self._declare_and_bind_queues()

    def _declare_exchanges(self):
        self._log(logging.DEBUG, "(re)declaring exchanges...")
        for config in [self._error_config, self._event_config, self._result_config]:
            self._channel_.exchange_declare(
                exchange=config.exchange.name,
                exchange_type=config.exchange.type,
                durable=True,
            )

    def _declare_and_bind_queues(self):
        self._log(logging.DEBUG, "(re)declaring queues...")
        for config in [self._error_config, self._event_config, self._result_config]:
            self._channel.queue_declare(config.default_queue, durable=True)
            self._channel.queue_bind(
                queue=config.default_queue,
                exchange=config.exchange.name,
                routing_key=config.routing_key,
            )

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
