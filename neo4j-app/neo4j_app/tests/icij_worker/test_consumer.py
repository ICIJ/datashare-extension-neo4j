from __future__ import annotations

import functools
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Type

import pytest
from pika import BasicProperties, BlockingConnection, DeliveryMode, URLParameters
from pika.channel import Channel
from pika.exceptions import StreamLostError
from pika.spec import Basic

from neo4j_app.icij_worker.exceptions import ConnectionLostError
from neo4j_app.tests.icij_worker.conftest import (
    TestConsumer__,
    consumer_factory,
    async_true_after,
    queue_exists,
    shutdown_nowait,
    true_after,
)


def _do_nothing(
    consumer,
    basic_deliver: Basic.Deliver,
    properties: BasicProperties,
    body: bytes,
):
    # pylint: disable=unused-argument
    pass


class _FatalError(ValueError):
    ...


class _RecoverableError(ValueError):
    ...


class ConnectionLostConsumer__(TestConsumer__):  # pylint: disable=invalid-name
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


class RecoverableErrorConsumer__(TestConsumer__):  # pylint: disable=invalid-name
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


class FatalErrorConsumer__(TestConsumer__):  # pylint: disable=invalid-name
    def on_message(
        self,
        _unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        raise _FatalError("this is too fatal i can't recover from this")


@pytest.mark.asyncio
async def test_consumer_should_consume(
    rabbit_mq: str,
    amqp_loggers,  # pylint: disable=unused-argument
):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    exchange = "default-ex"
    routing_key = "test"
    consumer_cls = consumer_factory(TestConsumer__, n_failures=0)
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

    with shutdown_nowait(ThreadPoolExecutor()) as executor:
        with consumer:
            executor.submit(consumer.consume)
            has_queue = functools.partial(queue_exists, queue)
            await async_true_after(has_queue, after_s=1.0)

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
        (2, ConnectionLostConsumer__),
        (2, RecoverableErrorConsumer__),
    ],
)
@pytest.mark.asyncio
async def test_consumer_should_reconnect_for_recoverable_error(
    rabbit_mq: str,
    n_failures: int,
    consumer_cls_: Type[TestConsumer__],
    amqp_loggers,  # pylint: disable=unused-argument
):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    exchange = "default-ex"
    routing_key = "test"
    test_consumer_cls = consumer_factory(consumer_cls_, n_failures)
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

    with shutdown_nowait(ThreadPoolExecutor()) as executor:
        with consumer:
            executor.submit(consumer.consume)
            has_queue = functools.partial(queue_exists, queue)
            await async_true_after(has_queue, after_s=1.0)

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


@pytest.mark.asyncio
async def test_consumer_should_not_reconnect_on_fatal_error(
    rabbit_mq: str,
    amqp_loggers,  # pylint: disable=unused-argument
):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    exchange = "default-ex"
    routing_key = "test"
    test_consumer_cls = consumer_factory(FatalErrorConsumer__, 0)
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
    with shutdown_nowait(ThreadPoolExecutor()) as executor:
        with consumer:
            future_res = executor.submit(consumer.consume)
            has_queue = functools.partial(queue_exists, queue)
            await async_true_after(has_queue, after_s=1.0)

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


@pytest.mark.asyncio
async def test_consumer_should_not_reconnect_too_many_times_when_inactive(
    rabbit_mq: str,
):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    exchange = "default-ex"
    routing_key = "test"
    n_failures = 10
    max_connection_attempts = 1
    inactive_after_s = 0  # Let's trigger the inactivity
    test_consumer_cls = consumer_factory(RecoverableErrorConsumer__, n_failures)
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

    with shutdown_nowait(ThreadPoolExecutor()) as executor:
        with consumer:
            future_res = executor.submit(consumer.consume)
            has_queue = functools.partial(queue_exists, queue)
            await async_true_after(has_queue, after_s=1.0)

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
