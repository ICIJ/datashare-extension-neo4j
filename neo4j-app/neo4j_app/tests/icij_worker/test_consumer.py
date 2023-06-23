from __future__ import annotations

import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Type

import pytest
from pika import (
    BasicProperties,
    BlockingConnection,
    DeliveryMode,
    URLParameters,
)
from pika.channel import Channel
from pika.exceptions import StreamLostError
from pika.spec import Basic

from neo4j_app.icij_worker.consumer import MessageConsumer, _MessageConsumer
from neo4j_app.icij_worker.exceptions import ConnectionLostError
from neo4j_app.tests.icij_worker.conftest import true_after


def _do_nothing(_: bytes):
    pass


@contextmanager
def _shutdown_nowait(executor: ThreadPoolExecutor):
    try:
        yield executor
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


class _TestConsumer_(_MessageConsumer):  # pylint: disable=invalid-name
    n_failures: int = 0
    consumed = 0

    def on_message(
        self,
        _unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        # pylint: disable=arguments-renamed
        super().on_message(_unused_channel, basic_deliver, properties, body)
        self.consumed += 1


def _consumer_factory(consumer_cls: Type[_TestConsumer_], n_failures: int) -> Type:
    consumer_cls.n_failures = n_failures

    class TestConsumer(MessageConsumer):
        @property
        def consumed(self) -> int:
            return self._consumer.consumed

        @property
        def consumer(self) -> _TestConsumer_:
            return self._consumer

        def _create_consumer(self) -> _TestConsumer_:
            return consumer_cls(
                on_message=self._on_message,
                name=self._name,
                exchange=self._exchange,
                broker_url=self._broker_url,
                queue=self._queue,
                routing_key=self._routing_key,
                app_id=self._app_id,
                recover_from=self._recover_from,
            )

    return TestConsumer


class _FatalError(ValueError):
    ...


class _RecoverableError(ValueError):
    ...


class _ConnectionLostConsumer_(_TestConsumer_):  # pylint: disable=invalid-name
    def on_message(
        self,
        _unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        self._last_message_received_at = time.monotonic()
        if self.__class__.n_failures:
            self.__class__.n_failures -= 1
            self._connection._proto_eof_received()  # pylint: disable=protected-access
        super().on_message(_unused_channel, basic_deliver, properties, body)


class _RecoverableErrorConsumer_(_TestConsumer_):  # pylint: disable=invalid-name
    def on_message(
        self,
        _unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        self._last_message_received_at = time.monotonic()
        if self.__class__.n_failures:
            self.__class__.n_failures -= 1
            raise _RecoverableError("i can recover from this")
        super().on_message(_unused_channel, basic_deliver, properties, body)


class _FatalErrorConsumer_(_TestConsumer_):  # pylint: disable=invalid-name
    def on_message(
        self,
        _unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        raise _FatalError("this is too fatal i can't recover from this")


def test_consumer_should_consume(
    rabbit_mq: str,
    amqp_loggers,  # pylint: disable=unused-argument
):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    exchange = "default-ex"
    routing_key = "test"
    consumer_cls = _consumer_factory(_TestConsumer_, n_failures=0)
    consumer = consumer_cls(
        on_message=_do_nothing,
        name="test-consumer",
        exchange=exchange,
        broker_url=broker_url,
        queue=queue,
        routing_key=routing_key,
        max_connection_wait_s=0.1,
        max_connection_attempts=5,
    )

    with _shutdown_nowait(ThreadPoolExecutor()) as executor:
        with consumer:
            executor.submit(consumer.consume)
            # When
            with BlockingConnection(URLParameters(broker_url)) as connection:
                with connection.channel() as channel:
                    channel.basic_publish(
                        exchange,
                        routing_key,
                        b"",
                        BasicProperties(
                            content_type="text/plain",
                            delivery_mode=DeliveryMode.Transient,
                        ),
                    )

                # Then
                after_s = 1.0
                statement = (
                    lambda: consumer.consumed  # pylint: disable=unnecessary-lambda-assignment
                )
                msg = f"consumer failed to consume within {after_s}s"
                assert true_after(statement, after_s=after_s), msg


@pytest.mark.parametrize(
    "n_failures,consumer_cls_",
    [
        (2, _ConnectionLostConsumer_),
        (2, _RecoverableErrorConsumer_),
    ],
)
def test_consumer_should_reconnect_for_recoverable_error(
    rabbit_mq: str,
    n_failures: int,
    consumer_cls_: Type[_TestConsumer_],
    amqp_loggers,  # pylint: disable=unused-argument
):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    exchange = "default-ex"
    routing_key = "test"
    test_consumer_cls = _consumer_factory(consumer_cls_, n_failures)
    recover_from = (_RecoverableError, ConnectionLostError)
    consumer = test_consumer_cls(
        on_message=_do_nothing,
        name="test-consumer",
        exchange=exchange,
        broker_url=broker_url,
        queue=queue,
        routing_key=routing_key,
        max_connection_wait_s=0.1,
        max_connection_attempts=5,
        recover_from=recover_from,
    )

    with _shutdown_nowait(ThreadPoolExecutor()) as executor:
        with consumer:
            executor.submit(consumer.consume)
            # When
            with BlockingConnection(URLParameters(broker_url)) as connection:
                with connection.channel() as channel:
                    channel.basic_publish(
                        exchange,
                        routing_key,
                        b"",
                        BasicProperties(
                            content_type="text/plain",
                            delivery_mode=DeliveryMode.Transient,
                        ),
                    )

                # Then
                after_s = 1.0
                statement = (
                    lambda: consumer.consumed  # pylint: disable=unnecessary-lambda-assignment
                )
                msg = f"consumer failed to consume within {after_s}s"
                assert true_after(statement, after_s=after_s), msg


def test_consumer_should_not_reconnect_on_fatal_error(
    rabbit_mq: str,
    amqp_loggers,  # pylint: disable=unused-argument
):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    exchange = "default-ex"
    routing_key = "test"
    test_consumer_cls = _consumer_factory(_FatalErrorConsumer_, 0)
    consumer = test_consumer_cls(
        on_message=_do_nothing,
        name="test-consumer",
        exchange=exchange,
        broker_url=broker_url,
        queue=queue,
        routing_key=routing_key,
        max_connection_wait_s=0.1,
        max_connection_attempts=5,
    )
    with _shutdown_nowait(ThreadPoolExecutor()) as executor:
        with consumer:
            future_res = executor.submit(consumer.consume)

            # When
            with BlockingConnection(URLParameters(broker_url)) as connection:
                with connection.channel() as channel:
                    channel.basic_publish(
                        exchange,
                        routing_key,
                        b"",
                        BasicProperties(
                            content_type="text/plain",
                            delivery_mode=DeliveryMode.Transient,
                        ),
                    )

                # Then
                with pytest.raises(StreamLostError) as exc:
                    future_res.result()
                    assert exc.match(
                        '_FatalError("this is too fatal i can\'t recover from this")'
                    )


def test_consumer_should_not_reconnect_too_many_times_when_inactive(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    exchange = "default-ex"
    routing_key = "test"
    n_failures = 10
    max_connection_attempts = 1
    inactive_after_s = 0  # Let's trigger the inactivity
    test_consumer_cls = _consumer_factory(_RecoverableErrorConsumer_, n_failures)
    recover_from = (_RecoverableError,)
    consumer = test_consumer_cls(
        on_message=_do_nothing,
        name="test-consumer",
        exchange=exchange,
        broker_url=broker_url,
        queue=queue,
        routing_key=routing_key,
        max_connection_wait_s=0.1,
        max_connection_attempts=max_connection_attempts,
        inactive_after_s=inactive_after_s,
        recover_from=recover_from,
    )

    with _shutdown_nowait(ThreadPoolExecutor()) as executor:
        with consumer:
            future_res = executor.submit(consumer.consume)

            # When
            with BlockingConnection(URLParameters(broker_url)) as connection:
                with connection.channel() as channel:
                    channel.basic_publish(
                        exchange,
                        routing_key,
                        b"",
                        BasicProperties(
                            content_type="text/plain",
                            delivery_mode=DeliveryMode.Transient,
                        ),
                    )

                    # Then
                    with pytest.raises(_RecoverableError):
                        future_res.result()


def test_consumer_should_close_gracefully_on_sigint(rabbit_mq: str):
    # Given
    main_test_path = Path(__file__).parent / "consumer_main.py"
    cmd = [sys.executable, main_test_path, rabbit_mq]

    # Then
    with Popen(cmd, stderr=PIPE, stdout=PIPE, text=True) as p:
        # Wait for the consumer to be running
        assert true_after(
            lambda: any("starting consuming" in l for l in p.stderr), after_s=2.0
        ), "Failed to start consumer"
        # Kill it
        p.send_signal(signal.SIGINT)
        assert true_after(
            lambda: any("shutting down gracefully" in l for l in p.stderr), after_s=2.0
        ), "Failed to shutdown consumer gracefully"


def test_consumer_should_close_immediately_on_sigterm(rabbit_mq: str):
    # Given
    main_test_path = Path(__file__).parent / "consumer_main.py"
    cmd = [sys.executable, main_test_path, rabbit_mq]

    # Then
    with Popen(cmd, stderr=PIPE, stdout=PIPE, text=True) as p:
        # Wait for the consumer to be running
        assert true_after(
            lambda: any("starting consuming" in l for l in p.stderr), after_s=2.0
        ), "Failed to start consumer"
        # Kill it
        p.send_signal(signal.SIGTERM)
        assert true_after(
            lambda: any("shutting down the hard way" in l for l in p.stderr),
            after_s=2.0,
        ), "Failed to shutdown consumer immediately"
