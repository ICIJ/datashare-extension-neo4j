from datetime import datetime
from typing import Optional

import pytest

from icij_worker.task import (
    PRECEDENCE,
    READY_STATES,
    Task,
    TaskError,
    TaskEvent,
    TaskStatus,
)

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
        (TaskStatus.QUEUED, TaskStatus.ERROR, TaskStatus.ERROR),
        (TaskStatus.QUEUED, TaskStatus.DONE, TaskStatus.DONE),
        # Late retry notice but the task is already failed
        (TaskStatus.ERROR, TaskStatus.QUEUED, TaskStatus.ERROR),
    ]
    + _DONE_IS_DONE
    + _SHOULD_CANCEL_UNREADY,
)
def test_resolve_status(
    stored: TaskStatus, event_status: TaskStatus, expected_resolved: TaskStatus
):
    # Given
    task = Task(
        id="some_id", status=stored, type="some-type", created_at=datetime.now()
    )
    event = TaskEvent(task_id=task.id, status=event_status)
    # When
    resolved = TaskStatus.resolve_event_status(task, event)
    # Then
    assert resolved == expected_resolved


@pytest.mark.parametrize(
    "task_retries,event_retries,expected",
    [
        # Delayed queued event
        (None, None, TaskStatus.RUNNING),
        (1, None, TaskStatus.RUNNING),
        (2, 1, TaskStatus.RUNNING),
        # The event is signaling a retry
        (None, 1, TaskStatus.QUEUED),
        (1, 2, TaskStatus.QUEUED),
    ],
)
def test_resolve_running_queued_status(
    task_retries: Optional[int], event_retries: Optional[int], expected: TaskStatus
):
    # Given
    task = Task(
        id="some_id",
        status=TaskStatus.RUNNING,
        type="some-type",
        created_at=datetime.now(),
        retries=task_retries,
    )
    event = TaskEvent(task_id=task.id, status=TaskStatus.QUEUED, retries=event_retries)
    # When
    resolved = TaskStatus.resolve_event_status(task, event)
    # Then
    assert resolved == expected
