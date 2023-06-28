import functools
import logging
import traceback
from inspect import signature
from typing import Callable, Optional, Tuple, Type

from pika.spec import Basic, BasicProperties
from pydantic import ValidationError, parse_raw_as

from neo4j_app.icij_worker import MessagePublisher
from neo4j_app.icij_worker.app import ICIJApp, RegisteredTask
from neo4j_app.icij_worker.exceptions import (
    InvalidTaskBody,
    MaxRetriesExceeded,
    UnregisteredTask,
)
from neo4j_app.icij_worker.task import (
    PROGRESS_HANDLER_ARG,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from neo4j_app.icij_worker.typing import ConsumerProtocol


def task_wrapper(
    basic_deliver: Basic.Deliver,
    properties: BasicProperties,
    body: bytes,
    *,
    consumer: ConsumerProtocol,
    publisher: MessagePublisher,
    app: ICIJApp,
):
    task = _parse_task_body(body)
    try:
        task_fn, recoverable_errors = _parse_task(app, publisher, task)
        _check_retries(app, task, properties, consumer)
        try:
            task_res = task_fn(**task.inputs)
        except recoverable_errors as e:
            consumer.log(
                logging.ERROR, "%s(id=%s) recovering from: %s", task.type, task.id, e
            )
            # TODO: handle retry here...
            event = TaskEvent(
                task_id=task.id, error=_format_error(e), status=TaskStatus.RETRY
            )
            publisher.publish_task_event(event, mandatory=False)
            # TODO: for now we requeue the task to its original queue. When retries will
            #  be implemented, we'll probably have to put the message in the DLX using
            #  requeue = False
            consumer.reject_message(
                delivery_tag=basic_deliver.delivery_tag, requeue=True
            )
            return
        consumer.log(
            logging.INFO, "publishing results for %s(id=%s)", task.type, task.id
        )
        result = TaskResult(task_id=task.id, result=task_res)
        publisher.publish_task_result(result)
        consumer.log(
            logging.INFO, "marking %s(id=%s) as %s", task.type, task.id, TaskStatus.DONE
        )
        event = TaskEvent(
            task_id=task.id, status=TaskStatus.DONE, progress=100, error=None
        )
        publisher.publish_task_event(event, mandatory=False)
        consumer.log(logging.INFO, "task %s successful !", task.id)
    except MaxRetriesExceeded as e:
        consumer.log(logging.ERROR, e)
        event = TaskEvent(
            task_id=task.id, error=_format_error(e), status=TaskStatus.ERROR
        )
        publisher.publish_task_event(event, mandatory=False)
        publisher.publish_task_error(TaskError.from_exception(e, task_id=task.id))
        # Remove message from queue
        consumer.acknowledge_message(delivery_tag=basic_deliver.delivery_tag)
        consumer.log(
            logging.ERROR, "%s(id=%s) definitively rejected", task.type, task.id
        )
        # Acknowledgement left to the outer scope
    except Exception as e:  # pylint: disable=broad-exception-caught
        consumer.log(logging.ERROR, "task %s(id=%s), fatal error: %s", task.type, e)
        event = TaskEvent(
            task_id=task.id, error=_format_error(e), status=TaskStatus.ERROR
        )
        publisher.publish_task_event(event, mandatory=False)
        publisher.publish_task_error(TaskError.from_exception(e, task_id=task.id))
        consumer.reject_message(delivery_tag=basic_deliver.delivery_tag, requeue=False)
        consumer.log(
            logging.INFO, "%s(id=%s) definitively rejected", task.type, task.id
        )
    # Acknowledgement left to the outer scope


def _parse_task(
    app: ICIJApp, publisher: MessagePublisher, task: Task
) -> Tuple[Callable, Tuple[Type[Exception], ...]]:
    registered = _retrieve_registered_task(task, app)
    recoverable = registered.recover_from
    task_fn = registered.task
    supports_progress = any(
        param.name == PROGRESS_HANDLER_ARG
        for param in signature(task_fn).parameters.values()
    )
    if supports_progress:
        publish_progress = functools.partial(
            _publish_progress,
            functools.partial(publisher.publish_task_event, mandatory=False),
            task_id=task.id,
        )
        task_fn = functools.partial(task_fn, progress_handler=publish_progress)
    return task_fn, recoverable


def _parse_task_body(body: bytes) -> Task:
    try:
        task = parse_raw_as(Task, body)
    except ValidationError as e:
        displayed_body = body[:1000]
        error = InvalidTaskBody(f"Invalid task body {displayed_body}")
        raise error from e
    return task


def _retrieve_registered_task(
    task: Task,
    app: ICIJApp,
) -> RegisteredTask:
    registered = app.registry.get(task.type)
    if registered is None:
        available_tasks = list(app.registry)
        raise UnregisteredTask(task.type, available_tasks)
    return registered


def _publish_progress(
    publish_task_event: Callable[[TaskEvent], None], progress: float, task_id: str
):
    updated = TaskEvent(progress=progress, task_id=task_id)
    publish_task_event(updated)


def _parse_retries(properties: BasicProperties) -> Optional[int]:
    # pylint: disable=unused-argument
    return None


def _check_retries(
    app: ICIJApp, task: Task, properties: BasicProperties, consumer: ConsumerProtocol
):
    # TODO: here we're retrying endlessly, in the future we should parse the message
    #  header and raise a MaxRetriesExceeded when relevant
    max_retries = app.registry[task.type].max_retries
    retries = _parse_retries(properties)  # pylint: disable=assignment-from-none
    consumer.log(
        logging.INFO, "%s(id=%s): try %s/%s", task.type, task.id, retries, max_retries
    )
    if retries is not None and retries > max_retries:
        raise MaxRetriesExceeded(
            f"{task.type}(id={task.id}): max retries exceeded > {max_retries}"
        )


def _format_error(error: Exception) -> str:
    formatted = "".join(
        traceback.format_exception(None, value=error, tb=error.__traceback__)
    )
    return formatted
