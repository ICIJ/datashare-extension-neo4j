from datetime import datetime
from typing import Optional

import pytest

from neo4j_app.icij_worker import Task, TaskError, TaskEvent, TaskStatus
from neo4j_app.icij_worker.task import PRECEDENCE, READY_STATES

_CREATED_AT = datetime.now()
_ERROR_OCCURRED_AT = datetime.now()
_ANOTHER_TIME = datetime.now()


def test_precedence_sanity_check():
    assert len(PRECEDENCE) == len(TaskStatus)


@pytest.mark.parametrize(
    "task,event,expected_resolved",
    [
        # Update the status
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", status=TaskStatus.RUNNING),
            TaskEvent(task_id="task-id", status=TaskStatus.RUNNING),
        ),
        # Task type is not updated
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", task_type="goodbye_world"),
            None,
        ),
        # Status is updated when not in a final state
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", status=TaskStatus.QUEUED),
            TaskEvent(task_id="task-id", status=TaskStatus.QUEUED),
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.QUEUED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", status=TaskStatus.RUNNING),
            TaskEvent(task_id="task-id", status=TaskStatus.RUNNING),
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.RUNNING,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", status=TaskStatus.DONE),
            TaskEvent(task_id="task-id", status=TaskStatus.DONE),
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.RETRY,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", status=TaskStatus.ERROR),
            TaskEvent(task_id="task-id", status=TaskStatus.ERROR),
        ),
        # Update the progress
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", progress=50.0),
            TaskEvent(task_id="task-id", progress=50.0),
        ),
        # Update retries
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", retries=4),
            TaskEvent(task_id="task-id", retries=4),
        ),
        # Update error
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(
                task_id="task-id",
                error=TaskError(
                    id="error-id",
                    title="some-error",
                    detail="some details",
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
            ),
            TaskEvent(
                task_id="task-id",
                error=TaskError(
                    id="error-id",
                    title="some-error",
                    detail="some details",
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
            ),
        ),
        # Created at is not updated
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", created_at=_ANOTHER_TIME),
            None,
        ),
        # Completed at is not updated
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.DONE,
                created_at=_CREATED_AT,
                completed_at=_CREATED_AT,
            ),
            TaskEvent(task_id="task-id", created_at=_ANOTHER_TIME),
            None,
        ),
        # The task is on a final state, nothing is updated
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.DONE,
                created_at=_CREATED_AT,
            ),
            TaskEvent(
                task_id="task-id",
                task_type="goodbye_world",
                status=TaskStatus.RUNNING,
                progress=50.0,
                retries=4,
                error=TaskError(
                    id="error-id",
                    title="some-error",
                    detail="some details",
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
                created_at=_ANOTHER_TIME,
                completed_at=_ANOTHER_TIME,
            ),
            None,
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.ERROR,
                created_at=_CREATED_AT,
            ),
            TaskEvent(
                task_id="task-id",
                task_type="goodbye_world",
                status=TaskStatus.RUNNING,
                progress=50.0,
                retries=4,
                error=TaskError(
                    id="error-id",
                    title="some-error",
                    detail="some details",
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
                created_at=_ANOTHER_TIME,
                completed_at=_ANOTHER_TIME,
            ),
            None,
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CANCELLED,
                created_at=_CREATED_AT,
            ),
            TaskEvent(
                task_id="task-id",
                task_type="goodbye_world",
                status=TaskStatus.RUNNING,
                progress=50.0,
                retries=4,
                error=TaskError(
                    id="error-id",
                    title="some-error",
                    detail="some details",
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
                created_at=_ANOTHER_TIME,
                completed_at=_ANOTHER_TIME,
            ),
            None,
        ),
    ],
)
def test_resolve_event(
    task: Task, event: TaskEvent, expected_resolved: Optional[TaskEvent]
):
    # When
    resolved = task.resolve_event(event)
    # Then
    assert resolved == expected_resolved


_UNCHANGED = [(s, s, s) for s in TaskStatus]
_DONE_IS_DONE = [
    (TaskStatus.DONE, s, TaskStatus.DONE) for s in TaskStatus if s != TaskStatus.DONE
]
_SHOULD_CANCEL_UNREADY = [
    (s, TaskStatus.CANCELLED, TaskStatus.CANCELLED)
    for s in TaskStatus
    if s not in READY_STATES
]


@pytest.mark.parametrize(
    "stored,event_status,expected_resolved",
    _UNCHANGED
    + [
        (TaskStatus.CREATED, TaskStatus.QUEUED, TaskStatus.QUEUED),
        # Store as queue, receiving a late creation event, the task stays queue
        (TaskStatus.QUEUED, TaskStatus.CREATED, TaskStatus.QUEUED),
        (TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.RUNNING),
        (TaskStatus.RUNNING, TaskStatus.RETRY, TaskStatus.RETRY),
        (TaskStatus.RETRY, TaskStatus.ERROR, TaskStatus.ERROR),
        (TaskStatus.RETRY, TaskStatus.DONE, TaskStatus.DONE),
        # Late running notice but the task is already retried
        (TaskStatus.RETRY, TaskStatus.RUNNING, TaskStatus.RETRY),
        # Late retry notice but the task is already failed
        (TaskStatus.ERROR, TaskStatus.RETRY, TaskStatus.ERROR),
    ]
    + _DONE_IS_DONE
    + _SHOULD_CANCEL_UNREADY,
)
def test_resolve_status(
    stored: TaskStatus, event_status: TaskStatus, expected_resolved: TaskStatus
):
    # When
    resolved = TaskStatus.resolve_event_status(stored=stored, event_status=event_status)
    # Then
    assert resolved == expected_resolved
