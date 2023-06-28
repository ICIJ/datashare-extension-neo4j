import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional
from unittest.mock import MagicMock, call

import pytest
from pika import BlockingConnection, DeliveryMode, URLParameters
from pika.exchange_type import ExchangeType
from pika.spec import Basic, BasicProperties

from neo4j_app import icij_worker
from neo4j_app.icij_worker import (
    Exchange,
    ICIJApp,
    MessageConsumer,
    MessagePublisher,
    Routing,
)
from neo4j_app.icij_worker.task import Task, TaskEvent, TaskResult, TaskStatus
from neo4j_app.icij_worker.typing import ProgressHandler
from neo4j_app.icij_worker.worker import Worker, task_wrapper
from neo4j_app.tests.icij_worker.conftest import (
    TestConsumer__,
    async_true_after,
    consumer_factory,
    queue_exists,
    shutdown_nowait,
    true_after,
)

mock_app = ICIJApp(name="mock_app")


class _TestWorker(Worker):
    @property
    def consumer(self) -> MessageConsumer:
        return self._consumer


@mock_app.task("hello_world")
def _hello_word(
    greeted: str, progress_handler: Optional[ProgressHandler] = None
) -> str:
    progress_handler(0.0)
    greeting = f"Hello {greeted} !"
    progress_handler(100.0)
    return greeting


class _Recovering(ValueError):
    pass


@mock_app.task("recovering_task", recover_from=(_Recovering,))
def _recovering_task() -> str:
    raise _Recovering("i can recover from this")


@mock_app.task("fatal_error_task")
def _fatal_error_task() -> str:
    raise ValueError("this is fatal")


def test_task_wrapper_should_publish_result():
    # pylint: disable=pointless-statement
    # Given
    mocked_consumer = MagicMock()
    mocked_publisher = MagicMock()
    deliver = Basic.Deliver(delivery_tag="some-tag")
    properties = BasicProperties()
    task = Task(
        id="some-id",
        type="hello_world",
        created_at=datetime.now().isoformat(),
        status=TaskStatus.CREATED,
        inputs={"greeted": "world"},
    )
    body = task.json().encode()

    # When
    task_wrapper(
        basic_deliver=deliver,
        properties=properties,
        consumer=mocked_consumer,
        publisher=mocked_publisher,
        app=mock_app,
        body=body,
    )

    # Then
    mocked_consumer.acknowledge_message.assert_not_called
    mocked_consumer.reject_message.assert_not_called
    mocked_consumer.log.assert_called_with(
        logging.INFO, "task %s successful !", task.id
    )
    expected_results = TaskResult(task_id=task.id, result="Hello world !")
    mocked_publisher.publish_task_result.assert_called_with(expected_results)
    expected_events = [
        call(TaskEvent(task_id="some-id", progress=0.0), mandatory=False),
        call(TaskEvent(task_id="some-id", progress=100.0), mandatory=False),
        call(
            TaskEvent(
                task_id="some-id",
                status=TaskStatus.DONE,
                progress=100.0,
            ),
            mandatory=False,
        ),
    ]
    assert mocked_publisher.publish_task_event.call_args_list == expected_events
    mocked_publisher.publish_task_error.assert_not_called


def test_task_wrapper_should_recover_from_recoverable_error():
    # pylint: disable=pointless-statement
    # Given
    mocked_consumer = MagicMock()
    mocked_publisher = MagicMock()
    deliver = Basic.Deliver(delivery_tag="some-tag")
    properties = BasicProperties()
    task = Task(
        id="some-id",
        type="recovering_task",
        created_at=datetime.now().isoformat(),
        status=TaskStatus.CREATED,
    )
    body = task.json().encode()

    # When
    task_wrapper(
        basic_deliver=deliver,
        properties=properties,
        consumer=mocked_consumer,
        publisher=mocked_publisher,
        app=mock_app,
        body=body,
    )

    # Then
    mocked_consumer.acknowledge_message.assert_not_called
    mocked_consumer.reject_message.assert_called_with(
        delivery_tag=deliver.delivery_tag, requeue=True
    )
    mocked_publisher.publish_task_result.assert_not_called
    assert mocked_publisher.publish_task_event.call_count == 1
    published_event = mocked_publisher.publish_task_event.mock_calls[0].args[0]
    assert published_event.task_id == "some-id"
    assert published_event.status is TaskStatus.RETRY
    assert published_event.progress is None
    assert '_Recovering("i can recover from this")' in published_event.error
    mocked_publisher.publish_task_error.assert_not_called


def test_task_wrapper_should_handle_non_recoverable_error():
    # pylint: disable=pointless-statement
    # Given
    mocked_consumer = MagicMock()
    mocked_publisher = MagicMock()
    deliver = Basic.Deliver(delivery_tag="some-tag")
    properties = BasicProperties()
    task = Task(
        id="some-id",
        type="fatal_error_task",
        created_at=datetime.now().isoformat(),
        status=TaskStatus.CREATED,
    )
    body = task.json().encode()

    # When
    task_wrapper(
        basic_deliver=deliver,
        properties=properties,
        consumer=mocked_consumer,
        publisher=mocked_publisher,
        app=mock_app,
        body=body,
    )

    # Then
    mocked_consumer.acknowledge_message.assert_not_called
    mocked_consumer.reject_message.assert_called_with(
        delivery_tag=deliver.delivery_tag, requeue=False
    )
    mocked_publisher.publish_task_result.assert_not_called
    assert mocked_publisher.publish_task_event.call_count == 1
    published_event = mocked_publisher.publish_task_event.mock_calls[0].args[0]
    assert published_event.task_id == "some-id"
    assert published_event.progress is None
    assert published_event.status is TaskStatus.ERROR
    assert 'ValueError("this is fatal")' in published_event.error
    mocked_publisher.publish_task_error.assert_called_once
    published_error = mocked_publisher.publish_task_error.mock_calls[0].args[0]
    assert published_error.task_id == "some-id"
    assert published_error.title == "ValueError"
    assert 'ValueError("this is fatal")' in published_error.detail


def test_task_wrapper_should_handle_unregistered_task():
    # pylint: disable=pointless-statement
    # Given
    mocked_consumer = MagicMock()
    mocked_publisher = MagicMock()
    deliver = Basic.Deliver(delivery_tag="some-tag")
    properties = BasicProperties()
    task = Task(
        id="some-id",
        type="i_dont_exist",
        created_at=datetime.now().isoformat(),
        status=TaskStatus.CREATED,
    )
    body = task.json().encode()

    # When
    task_wrapper(
        basic_deliver=deliver,
        properties=properties,
        consumer=mocked_consumer,
        publisher=mocked_publisher,
        app=mock_app,
        body=body,
    )

    # Then
    mocked_consumer.acknowledge_message.assert_not_called
    mocked_consumer.reject_message.assert_called_with(
        delivery_tag=deliver.delivery_tag, requeue=False
    )
    mocked_publisher.publish_task_result.assert_not_called
    assert mocked_publisher.publish_task_event.call_count == 1
    published_event = mocked_publisher.publish_task_event.mock_calls[0].args[0]
    assert published_event.task_id == "some-id"
    assert published_event.progress is None
    assert published_event.status is TaskStatus.ERROR
    mocked_publisher.publish_task_error.assert_called_once
    expected_msg = 'UnregisteredTask task "i_dont_exist", available tasks: '
    assert published_event.error is not None
    assert expected_msg in published_event.error
    published_error = mocked_publisher.publish_task_error.mock_calls[0].args[0]
    assert published_error.task_id == "some-id"
    assert published_error.title == "UnregisteredTask"
    expected_msg = 'UnregisteredTask task "i_dont_exist", available tasks: '
    assert expected_msg in published_error.detail


@pytest.mark.asyncio
async def test_task_worker(rabbit_mq: str, amqp_loggers, monkeypatch):
    # pylint: disable=unused-argument
    # Given
    broker_url = rabbit_mq
    app_id = "datashare"
    event_routing = Routing(
        exchange=Exchange(name="event-ex", type=ExchangeType.fanout),
        routing_key="datashare.event.ping",
        default_queue="ping-queue",
    )
    error_routing = Routing(
        exchange=Exchange(name="error-ex", type=ExchangeType.topic),
        routing_key="datashare.error.ping",
        default_queue="error-queue",
    )
    result_routing = Routing(
        exchange=Exchange(name="result-ex", type=ExchangeType.topic),
        routing_key="datashare.result.ping",
        default_queue="result-queue",
    )
    task_routing = Routing(
        exchange=Exchange(name="task-ex", type=ExchangeType.topic),
        routing_key="datashare.task.ping",
        default_queue="task-queue",
    )
    publisher = MessagePublisher(
        name="test-publisher",
        event_routing=event_routing,
        error_routing=error_routing,
        result_routing=result_routing,
        broker_url=broker_url,
        app_id=app_id,
    )
    consumer_cls_factory = consumer_factory(TestConsumer__, n_failures=0)
    monkeypatch.setattr(icij_worker.worker, "MessageConsumer", consumer_cls_factory)
    worker = _TestWorker(
        name="test-worker",
        app=mock_app,
        task_routing=task_routing,
        publisher=publisher,
    )
    task = Task(
        id="some-id",
        type="hello_world",
        created_at=datetime.now().isoformat(),
        status=TaskStatus.CREATED,
        inputs={"greeted": "world"},
    )
    body = task.json().encode()

    # When
    with shutdown_nowait(ThreadPoolExecutor()) as executor:
        executor.submit(worker.work)
        has_queue = functools.partial(queue_exists, task_routing.default_queue)
        await async_true_after(has_queue, after_s=1.0)
        with BlockingConnection(URLParameters(broker_url)) as connection:
            with connection.channel() as channel:
                channel.basic_publish(
                    task_routing.exchange.name,
                    task_routing.routing_key,
                    body,
                    BasicProperties(
                        content_type="text/plain",
                        delivery_mode=DeliveryMode.Persistent,
                    ),
                )

                # Then
                after_s = 1.0
                statement = (
                    lambda: worker.consumer.consumed  # pylint: disable=unnecessary-lambda-assignment
                )
                msg = f"consumer failed to consume within {after_s}s"
                assert true_after(statement, after_s=after_s), msg
