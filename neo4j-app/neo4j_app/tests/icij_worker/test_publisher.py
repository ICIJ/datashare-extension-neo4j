from unittest.mock import MagicMock, call, patch

import pytest
from pika import BasicProperties, BlockingConnection, DeliveryMode, URLParameters
from pika.exceptions import (
    ConnectionOpenAborted,
    NackError,
    StreamLostError,
    UnroutableError,
)
from pika.exchange_type import ExchangeType

from neo4j_app import icij_worker
from neo4j_app.icij_worker import Exchange, Routing
from neo4j_app.icij_worker.publisher import MessagePublisher
from neo4j_app.icij_worker.task import TaskError, TaskEvent, TaskResult

_EVENT_ROUTING = Routing(
    exchange=Exchange(name="event-ex", type=ExchangeType.fanout),
    routing_key="event",
    default_queue="event-q",
)

_ERROR_ROUTING = Routing(
    exchange=Exchange(name="error-ex", type=ExchangeType.topic),
    routing_key="error",
    default_queue="error-q",
)

_RESULT_ROUTING = Routing(
    exchange=Exchange(name="result-ex", type=ExchangeType.topic),
    routing_key="result",
    default_queue="result-q",
)


class _TestablePublisher(MessagePublisher):
    @property
    def can_publish(self) -> bool:
        if self._connection_ is None or not self._connection.is_open:
            return False
        if self._channel_ is None or not self._channel.is_open:
            return False
        return True


@pytest.mark.parametrize("mandatory", [True, False])
@pytest.mark.asyncio
async def test_publisher_should_publish(rabbit_mq: str, mandatory: bool):
    # Given
    broker_url = rabbit_mq
    exchange = _EVENT_ROUTING.exchange.name
    queue = _EVENT_ROUTING.default_queue
    routing_key = _EVENT_ROUTING.routing_key
    message = "hello world"
    publisher = MessagePublisher(
        name="test-publisher",
        broker_url=broker_url,
        event_routing=_EVENT_ROUTING,
        result_routing=_RESULT_ROUTING,
        error_routing=_ERROR_ROUTING,
    )

    # When
    with publisher.connect():
        publisher._publish_message(  # pylint: disable=protected-access
            message.encode(),
            exchange=exchange,
            routing_key=routing_key,
            delivery_mode=DeliveryMode.Transient,
            mandatory=mandatory,
        )

    # Then
    connection = BlockingConnection(URLParameters(broker_url))
    channel = connection.channel()
    _, _, body = channel.basic_get(queue, auto_ack=True)
    assert body.decode() == message


@pytest.mark.parametrize(
    "error,n_disconnects",
    [
        (
            StreamLostError(f"Stream connection lost: {ConnectionError('error')!r}"),
            2,
        ),
        (UnroutableError([]), 3),
        (NackError([]), 4),
    ],
)
@pytest.mark.asyncio
async def test_publisher_should_reconnect_for_recoverable_error(
    rabbit_mq: str, error: Exception, n_disconnects: int
):
    # Given
    broker_url = rabbit_mq
    recover_from = (ConnectionError, UnroutableError, NackError)
    publisher = _TestablePublisher(
        name="test-publisher",
        event_routing=_EVENT_ROUTING,
        result_routing=_RESULT_ROUTING,
        error_routing=_ERROR_ROUTING,
        broker_url=broker_url,
        recover_from=recover_from,
    )
    max_attempt = 10

    # When
    success_i = None
    with publisher.connect():
        for i, attempt in enumerate(
            publisher.reconnection_attempts(max_attempt=max_attempt, max_wait_s=0.1)
        ):
            with attempt:
                if i < n_disconnects:
                    publisher.close()
                    raise error
            success_i = i

        # Then
        assert publisher.can_publish
        assert success_i == n_disconnects


def test_publisher_should_not_reconnect_on_fatal_error(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    publisher = MessagePublisher(
        name="test-publisher",
        event_routing=_EVENT_ROUTING,
        result_routing=_RESULT_ROUTING,
        error_routing=_ERROR_ROUTING,
        broker_url=broker_url,
    )
    max_attempt = 10

    class _MyNonRecoverableError(Exception):
        pass

    # When/Then
    with publisher.connect():
        with pytest.raises(_MyNonRecoverableError):
            for attempt in publisher.reconnection_attempts(
                max_attempt=max_attempt, max_wait_s=0.1
            ):
                with attempt:
                    raise _MyNonRecoverableError()


def test_publisher_should_not_reconnect_too_many_times(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    publisher = MessagePublisher(
        name="test-publisher",
        event_routing=_EVENT_ROUTING,
        result_routing=_RESULT_ROUTING,
        error_routing=_ERROR_ROUTING,
        broker_url=broker_url,
    )
    max_attempt = 2

    # When/Then
    with publisher.connect():
        with pytest.raises(ConnectionOpenAborted):
            for attempt in publisher.reconnection_attempts(
                max_attempt=max_attempt, max_wait_s=0.1
            ):
                with attempt:
                    raise ConnectionOpenAborted()


def test_publisher_should_create_and_bind_exchanges_and_queues():
    # pylint: disable=protected-access
    # Given
    broker_url = "amqp://guest:guest@localhost:666/vhost"
    mocked_connection = MagicMock()
    mocked_channel = MagicMock()
    mocked_connection.channel = MagicMock(return_value=mocked_channel)
    publisher = MessagePublisher(
        broker_url=broker_url,
        name="test-publisher",
        event_routing=_EVENT_ROUTING,
        result_routing=_RESULT_ROUTING,
        error_routing=_ERROR_ROUTING,
    )
    with patch.object(
        icij_worker.publisher, "BlockingConnection", new=mocked_connection
    ):
        # When
        with publisher.connect():
            # Then
            exchange_declared = publisher._channel.exchange_declare
            expected_exchange_calls = [
                call(
                    exchange="error-ex", exchange_type=ExchangeType.topic, durable=True
                ),
                call(
                    exchange="event-ex", exchange_type=ExchangeType.fanout, durable=True
                ),
                call(
                    exchange="result-ex", exchange_type=ExchangeType.topic, durable=True
                ),
            ]
            assert exchange_declared.call_args_list == expected_exchange_calls
            queue_declared = publisher._channel.queue_declare
            expected_queue_calls = [
                call("error-q", durable=True),
                call("event-q", durable=True),
                call("result-q", durable=True),
            ]
            assert queue_declared.call_args_list == expected_queue_calls
            queue_bind = publisher._channel.queue_bind
            expected_bind_calls = [
                call(queue="error-q", exchange="error-ex", routing_key="error"),
                call(queue="event-q", exchange="event-ex", routing_key="event"),
                call(queue="result-q", exchange="result-ex", routing_key="result"),
            ]
            assert queue_bind.call_args_list == expected_bind_calls


def test_publisher_publish_event():
    # pylint: disable=protected-access
    # Given
    broker_url = "amqp://guest:guest@localhost:666/vhost"
    mocked_connection = MagicMock()
    mocked_channel = MagicMock()
    mocked_connection.channel = MagicMock(return_value=mocked_channel)
    publisher = MessagePublisher(
        broker_url=broker_url,
        name="test-publisher",
        event_routing=_EVENT_ROUTING,
        result_routing=_RESULT_ROUTING,
        error_routing=_ERROR_ROUTING,
    )
    event = TaskEvent(task_id="some_task", progress=50.0)
    with patch.object(
        icij_worker.publisher, "BlockingConnection", new=mocked_connection
    ):
        # When
        with publisher.connect():
            publisher.publish_task_event(
                event,
                delivery_mode=DeliveryMode.Persistent,
                mandatory=True,
            )
            # Then
            basic_publish = publisher._channel.basic_publish
            serialized_event = b'{"task_id": "some_task", "status": null, \
"progress": 50.0, "error": null, "retries": null}'
            expected_call = call(
                "event-ex",
                "event",
                serialized_event,
                BasicProperties(delivery_mode=DeliveryMode.Persistent),
                mandatory=True,
            )
            assert basic_publish.call_args_list == [expected_call]


def test_publisher_publish_error():
    # pylint: disable=protected-access
    # Given
    broker_url = "amqp://guest:guest@localhost:666/vhost"
    mocked_connection = MagicMock()
    mocked_channel = MagicMock()
    mocked_connection.channel = MagicMock(return_value=mocked_channel)
    publisher = MessagePublisher(
        broker_url=broker_url,
        name="test-publisher",
        event_routing=_EVENT_ROUTING,
        result_routing=_RESULT_ROUTING,
        error_routing=_ERROR_ROUTING,
    )
    task_id = "some_task_id"

    with patch.object(
        icij_worker.publisher, "BlockingConnection", new=mocked_connection
    ):
        # When
        with publisher.connect():
            e = ValueError("some error here")
            task_error = None
            try:
                raise e
            except ValueError as ve:
                task_error = TaskError.from_exception(ve, task_id=task_id)
            publisher.publish_task_error(task_error)

            # Then
            basic_publish = publisher._channel.basic_publish
            serialized_error = f"{task_error.json()}".encode()
            expected_call = call(
                "error-ex",
                "error",
                serialized_error,
                BasicProperties(delivery_mode=DeliveryMode.Persistent),
                mandatory=True,
            )
            assert basic_publish.call_args_list == [expected_call]


def test_publisher_publish_result():
    # pylint: disable=protected-access
    # Given
    broker_url = "amqp://guest:guest@localhost:666/vhost"
    mocked_connection = MagicMock()
    mocked_channel = MagicMock()
    mocked_connection.channel = MagicMock(return_value=mocked_channel)
    publisher = MessagePublisher(
        broker_url=broker_url,
        name="test-publisher",
        event_routing=_EVENT_ROUTING,
        result_routing=_RESULT_ROUTING,
        error_routing=_ERROR_ROUTING,
    )
    result = TaskResult(
        task_id="some_task", result="some json serializable results here"
    )

    with patch.object(
        icij_worker.publisher, "BlockingConnection", new=mocked_connection
    ):
        # When
        with publisher.connect():
            publisher.publish_task_result(result)

            # Then
            basic_publish = publisher._channel.basic_publish
            serialized_result = f"{result.json()}".encode()
            expected_call = call(
                "result-ex",
                "result",
                serialized_result,
                BasicProperties(delivery_mode=DeliveryMode.Persistent),
                mandatory=True,
            )
            assert basic_publish.call_args_list == [expected_call]
