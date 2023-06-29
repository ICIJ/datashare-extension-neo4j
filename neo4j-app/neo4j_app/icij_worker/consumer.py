# Mostly inspired by
# https://github.com/pika/pika/blob/main/examples/asyncio_consumer_example.py
import functools
import logging
import signal
import threading
import time
from functools import cached_property
from typing import Dict, Optional, Tuple, Type

import pika
from pika import BaseConnection, BasicProperties, SelectConnection
from pika.channel import Channel
from pika.exceptions import (
    StreamLostError,
)
from pika.exchange_type import ExchangeType
from pika.frame import Method
from pika.spec import Basic

from neo4j_app.icij_worker import Routing
from neo4j_app.icij_worker.exceptions import (
    MaxReconnectionExceeded,
)
from neo4j_app.icij_worker.typing import OnMessage
from neo4j_app.icij_worker.utils import LogWithNameMixin, parse_stream_lost_error

_IOLOOP_ALREADY_RUNNING_MSG = "is already running"

logger = logging.getLogger(__name__)


class _MessageConsumer(LogWithNameMixin):
    _EXCHANGE_TYPE = ExchangeType.topic
    _logger = logger

    def __init__(
        self,
        *,
        on_message: OnMessage,
        name: str,
        broker_url: str,
        task_routing: Routing,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
    ):
        self._on_message = on_message

        self._name = name

        self._app_id = app_id
        self._broker_url = broker_url
        self._task_routing = task_routing
        self._recover_from = recover_from

        self._connection_ = None
        self._channel_ = None
        self._error = None

        self._should_reconnect = False
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._last_message_received_at = None
        # TODO: for now prefetch factor is disabled, for short tasks this might be
        #  helpful for a higher consumer throughput
        self._prefetch_count = 1

    @property
    def logged_name(self) -> str:
        return f"{self._name} (tag: {self.consumer_tag})"

    @property
    def should_reconnect(self) -> bool:
        return self._should_reconnect

    @property
    def error(self) -> Optional[Exception]:
        return self._error

    @property
    def last_message_received_at(self) -> Optional[float]:
        return self._last_message_received_at

    @property
    def consumer_tag(self) -> Optional[str]:
        return self._consumer_tag

    @property
    def _connection(self) -> SelectConnection:
        if self._connection_ is None:
            msg = (
                f"consumer has no connection, please call"
                f" {_MessageConsumer.connect.__name__}"
            )
            raise ValueError(msg)
        return self._connection_

    @property
    def _channel(self) -> Channel:
        if self._channel_ is None:
            msg = (
                f"consumer has no channel, please call"
                f" {_MessageConsumer.connect.__name__}"
            )
            raise ValueError(msg)
        return self._channel_

    @cached_property
    def _exception_namespace(self) -> Dict:
        ns = dict(globals())
        ns.update({exc_type.__name__: exc_type for exc_type in self._recover_from})
        return ns

    def connect(self):
        self._log(logging.DEBUG, "connecting to %s", self._broker_url)
        # TODO: heartbeat ? blocked connection timeout ?
        self._connection_ = SelectConnection(
            parameters=pika.URLParameters(self._broker_url),
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )

    def on_message(
        self,
        _unused_channel: Channel,  # pylint: disable=invalid-name
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,  # pylint: disable=invalid-name
        body: bytes,
    ):
        self._last_message_received_at = time.monotonic()
        msg = "received message # %s"
        if self._app_id is not None:
            msg = f"{msg} from {self._app_id}"
        self._log(logging.DEBUG, msg, basic_deliver.delivery_tag)
        self._on_message(basic_deliver=basic_deliver, properties=properties, body=body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def consume(self):
        self.connect()
        self._connection.ioloop.start()

    def reject_message(self, delivery_tag: int, requeue: bool):
        self._channel.basic_reject(delivery_tag=delivery_tag, requeue=requeue)

    def stop(self):
        if not self._closing:
            self._closing = True
            self._log(logging.INFO, "stopping...")
            if self._consuming:
                # The IOLoop is started again because this method is invoked  when
                # CTRL-C is pressed raising a KeyboardInterrupt exception. This
                # exception stops the IOLoop which needs to be running for pika to
                # communicate with RabbitMQ. All the commands issued prior to
                # starting the IOLoop will be buffered but not processed.
                self._stop_consuming()
                # Calling start will fail if the loop is already running
                try:
                    self._connection.ioloop.start()
                # not robust, but sadly pika does not provide a more catchable
                # exception type...
                except RuntimeError as e:
                    if _IOLOOP_ALREADY_RUNNING_MSG not in str(e):
                        raise e
            else:
                if self._connection_ is not None:
                    self._connection.ioloop.stop()
            self._log(logging.INFO, "stopped !")

    def _trigger_reconnect(self):
        self._should_reconnect = True

    def _stop_consuming(self):
        if self._channel:
            self._log(logging.DEBUG, "sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(self.consumer_tag, self._on_cancelok)

    def acknowledge_message(self, delivery_tag: int):
        self._log(logging.DEBUG, "acknowledging message %s", delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def _close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self._log(logging.DEBUG, "connection is closing or already closed")
        else:
            self._log(logging.DEBUG, "closing connection")
            self._connection.close()

    def _on_connection_open(self, _unused_connection: BaseConnection):
        # pylint: disable=invalid-name
        self._log(logging.DEBUG, "connection opened !")
        self._open_channel()

    def _on_connection_open_error(
        self, _unused_connection: BaseConnection, err: Exception
    ):
        # pylint: disable=invalid-name
        self._parse_error(err)
        if isinstance(self._error, StreamLostError):
            self._log(
                logging.WARNING, "failed to parse stream lost internal error: %s", err
            )
        self._log(logging.ERROR, "connection open failed: %s", self._error)
        if isinstance(self._error, self._recover_from):
            self._log(logging.ERROR, "triggering reconnection !")
            self._trigger_reconnect()
        self.stop()

    def _on_connection_closed(
        self, _unused_connection: BaseConnection, reason: Exception
    ):
        # pylint: disable=invalid-name
        self._parse_error(reason)
        self._channel_ = None
        if self._closing:  # The connection was closed on purpose
            self._connection.ioloop.stop()
        else:
            self._log(logging.ERROR, "connection was accidentally closed: %s", reason)
            if isinstance(self._error, self._recover_from):
                self._log(logging.ERROR, "triggering reconnection !")
                self._trigger_reconnect()
            self.stop()

    def _open_channel(self):
        self._log(logging.DEBUG, "creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel: Channel):
        self._log(logging.DEBUG, "channel opened !")
        self._channel_ = channel
        self._add_on_channel_close_callback()
        self._setup_exchange()

    def _add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel: Channel, reason: Exception):
        self._log(
            logging.WARNING, "channel %s was closed: %s", channel.channel_number, reason
        )
        self._close_connection()

    def _setup_exchange(self):
        self._log(logging.DEBUG, "declaring exchange: %s", self._task_routing.exchange)
        if self._task_routing.exchange.type is not ExchangeType.topic:
            raise ValueError(f"task exchange must be {ExchangeType.topic}")
        self._channel.exchange_declare(
            exchange=self._task_routing.exchange.name,
            exchange_type=self._task_routing.exchange.type,
            callback=self._on_exchange_declareok,
            durable=True,
        )

    def _on_exchange_declareok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self._log(
            logging.DEBUG, "exchange %s declared", self._task_routing.exchange.name
        )
        self._setup_queue()

    def _setup_queue(self):
        self._log(logging.DEBUG, "declaring queue %s", self._task_routing.default_queue)
        self._channel.queue_declare(
            queue=self._task_routing.default_queue,
            callback=self._on_queue_declareok,
            durable=True,
        )

    def _on_queue_declareok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self._log(
            logging.INFO,
            "binding %s to %s with %s",
            self._task_routing.exchange.name,
            self._task_routing.default_queue,
            self._task_routing.routing_key,
        )
        self._channel.queue_bind(
            self._task_routing.default_queue,
            self._task_routing.exchange.name,
            routing_key=self._task_routing.routing_key,
            callback=self._on_bindok,
        )

    def _on_bindok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self._log(logging.DEBUG, "queue %s bound", self._task_routing.default_queue)
        self._set_qos()

    def _set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self._on_basic_qos_ok
        )

    def _on_basic_qos_ok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self._log(logging.DEBUG, "QOS set to: %d", self._prefetch_count)
        self._start_consuming()

    def _start_consuming(self):
        self._log(logging.INFO, "starting consuming...")
        self._add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._task_routing.default_queue,
            self.on_message,
            consumer_tag=self.consumer_tag,
        )
        self._consuming = True

    def _add_on_cancel_callback(self):
        self._log(logging.DEBUG, "adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

    def _on_consumer_cancelled(self, method_frame: Method):
        # pylint: disable=invalid-name
        self._log(
            logging.ERROR,
            "consumer was cancelled remotely, shutting down: %r",
            method_frame,
        )
        if self._channel:
            self._channel.close()

    def _on_cancelok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self._consuming = False
        self._log(
            logging.INFO,
            "RabbitMQ acknowledged the cancellation of the consumer",
        )
        self._close_channel()
        self._log(logging.INFO, "exiting consumer execution after cancellation")

    def _close_channel(self):
        self._log(logging.DEBUG, "closing the channel")
        self._channel.close()

    def _parse_error(self, error: Exception):
        if isinstance(error, StreamLostError):
            self._error = parse_stream_lost_error(
                error, namespace=self._exception_namespace
            )
            if isinstance(self._error, StreamLostError):
                self._log(
                    logging.WARNING,
                    "failed to parse internal stream lost error %s",
                    error,
                )
        else:
            self._error = error


class MessageConsumer(LogWithNameMixin):
    _logger = logger

    def __init__(
        self,
        *,
        on_message: OnMessage,
        name: str,
        broker_url: str,
        task_routing: Routing,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
        max_connection_wait_s: float = 60.0,
        max_connection_attempts: int = 5,
        inactive_after_s: float = 60 * 60,
    ):
        self._on_message = functools.partial(on_message, consumer=self)
        self._name = name
        self._task_routing = task_routing
        self._broker_url = broker_url
        self._app_id = app_id
        self._recover_from = recover_from

        self._consumer = self._create_consumer()

        self._connection_attempt = 0
        self._max_wait_s = max_connection_wait_s
        self._max_attempts = max_connection_attempts
        # Before inactive_after_s second of activity the consumer is considered active
        # and we'll try to reconnect immediately. After that delay, we'll try to
        # reconnect at most max_connection_attempts
        self._inactive_after_s = inactive_after_s

        self._stop_gracefully = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def logged_name(self) -> str:
        return self._consumer.logged_name

    @property
    def consumer_tag(self) -> Optional[str]:
        return self._consumer.consumer_tag

    @property
    def _is_active(self) -> bool:
        if self._consumer.last_message_received_at is not None:
            elapsed = time.monotonic() - self._consumer.last_message_received_at
            return elapsed < self._inactive_after_s
        return False

    log = LogWithNameMixin._log

    def consume(self):
        logger.info("starting consuming...")
        self._setup_signal_handlers()
        try:
            while True:
                self._consumer.consume()
                # ioloop.stop() has been called, let's see if we should reconnect
                self._try_reconnect()
        except KeyboardInterrupt:
            self._log(logging.INFO, "shutting down...")
        except Exception as e:
            self._log(logging.ERROR, "error occurred while consuming: %s", e)
            self._log(logging.INFO, "will try to shutdown gracefully...")
            self._stop_gracefully = True
            raise e
        finally:
            self.close()

    def acknowledge_message(self, delivery_tag: int):
        self._consumer.acknowledge_message(delivery_tag=delivery_tag)

    def reject_message(self, delivery_tag: int, requeue: bool):
        self._consumer.reject_message(delivery_tag=delivery_tag, requeue=requeue)

    def close(self):
        if self._stop_gracefully:
            self._log(logging.INFO, "shutting down gracefully...")
            self._consumer.stop()
        else:
            self._log(logging.INFO, "shutting down the hard way...")

    def _create_consumer(self) -> _MessageConsumer:
        return _MessageConsumer(
            on_message=self._on_message,
            name=self._name,
            task_routing=self._task_routing,
            broker_url=self._broker_url,
            app_id=self._app_id,
            recover_from=self._recover_from,
        )

    def _try_reconnect(self):
        # if the consumer was already consuming, many AMQP steps occurred, the
        # disconnect occurred after a while, we can reset the connection attempt. If
        # the consumer didn't to this point we don't want to retry connecting forever,
        # we hence increment the counter
        if self._is_active:
            self._connection_attempt = 0
        error = self._consumer.error
        if self._consumer.should_reconnect:
            self._connection_attempt += 1
            if self._connection_attempt > self._max_attempts:
                msg = f"consumer exceeded {self._max_attempts} reconnections"
                try:
                    raise MaxReconnectionExceeded(msg)
                except MaxReconnectionExceeded as max_retry_exc:
                    raise error from max_retry_exc
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            self._log(
                logging.INFO,
                "reconnection attempt %i, reconnecting after %ds",
                self._connection_attempt,
                reconnect_delay,
            )
            time.sleep(reconnect_delay)
            self._consumer = self._create_consumer()
        else:
            self._log(
                logging.ERROR, "consumer encountered non recoverable error %s", error
            )
            raise error

    def _get_reconnect_delay(self) -> float:
        try:
            exp = 2 ** (self._connection_attempt - 1)
            result = 1 * exp
        except OverflowError:
            return self._max_wait_s
        return max(0, min(result, self._max_wait_s))

    def _signal_handler(
        self,
        signal_name: str,
        _,
        __,  # pylint: disable=invalid-name
        *,
        graceful: bool,
    ):
        self._log(logging.ERROR, "received %s", signal_name)
        self._stop_gracefully = graceful
        raise KeyboardInterrupt()

    def _setup_signal_handlers(self):
        if threading.current_thread() is threading.main_thread():
            signal.signal(
                signal.SIGINT,
                functools.partial(self._signal_handler, "SIGINT", graceful=True),
            )
            signal.signal(
                signal.SIGTERM,
                functools.partial(self._signal_handler, "SIGTERM", graceful=False),
            )
