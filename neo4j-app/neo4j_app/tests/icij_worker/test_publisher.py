import pika
import pytest
from pika import DeliveryMode, URLParameters
from pika.exceptions import (
    ConnectionOpenAborted,
    NackError,
    StreamLostError,
    UnroutableError,
)

from neo4j_app.icij_worker.publisher import MessagePublisher


class _TestablePublisher(MessagePublisher):
    @property
    def can_publish(self) -> bool:
        if self._connection_ is None or not self._connection.is_open:
            return False
        if self._channel_ is None or not self._channel.is_open:
            return False
        return True


@pytest.mark.parametrize("mandatory", [True, False])
def test_publisher_should_publish(rabbit_mq: str, mandatory: bool):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    publisher = MessagePublisher(
        name="test-publisher",
        exchange="default-ex",
        broker_url=broker_url,
        queue=queue,
        routing_key="test",
    )
    message = "hello world"

    # When
    with publisher.connect():
        publisher.publish_message(
            message.encode(), delivery_mode=DeliveryMode.Transient, mandatory=mandatory
        )

    # Then
    connection = pika.BlockingConnection(URLParameters(broker_url))
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
def test_publisher_should_reconnect_for_recoverable_error(
    rabbit_mq: str, error: Exception, n_disconnects: int
):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    recover_from = (ConnectionError, UnroutableError, NackError)
    publisher = _TestablePublisher(
        name="test-publisher",
        exchange="default-ex",
        broker_url=broker_url,
        queue=queue,
        routing_key="test",
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


def test_publisher_should_not_reconnect_from_non_recoverable_error(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    queue = "test-queue"
    publisher = MessagePublisher(
        name="test-publisher",
        exchange="default-ex",
        broker_url=broker_url,
        queue=queue,
        routing_key="test",
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
    queue = "test-queue"
    publisher = MessagePublisher(
        name="test-publisher",
        exchange="default-ex",
        broker_url=broker_url,
        queue=queue,
        routing_key="test",
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
