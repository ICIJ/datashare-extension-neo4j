# # pylint: disable=redefined-outer-name
from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest

from neo4j_app.icij_worker import (
    ICIJApp,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from neo4j_app.icij_worker.exceptions import TaskCancelled
from neo4j_app.icij_worker.worker.worker import add_missing_args, task_wrapper
from neo4j_app.tests.conftest import TEST_PROJECT, async_true_after
from neo4j_app.tests.icij_worker.conftest import MockManager, MockWorker


@pytest.fixture(scope="function")
def mock_failing_worker(test_failing_async_app: ICIJApp, tmpdir: Path) -> MockWorker:
    db_path = Path(tmpdir) / "db.json"
    MockWorker.fresh_db(db_path)
    lock = threading.Lock()
    worker = MockWorker(test_failing_async_app, "test-worker", db_path, lock)
    return worker


_TASK_DB = dict()


async def test_task_wrapper_run_asyncio_task(mock_worker: MockWorker):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, worker.db_lock, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        id="some-id",
        type="hello_world",
        created_at=created_at,
        status=TaskStatus.CREATED,
        inputs={"greeted": "world"},
    )

    # When
    await task_manager.enqueue(task, project)
    await task_wrapper(worker)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)
    saved_errors = await task_manager.get_task_errors(task_id=task.id, project=project)
    saved_result = await task_manager.get_task_result(task_id=task.id, project=project)

    # Then
    assert not saved_errors

    expected_task = Task(
        id="some-id",
        type="hello_world",
        progress=100,
        created_at=created_at,
        status=TaskStatus.DONE,
        inputs={"greeted": "world"},
    )
    completed_at = saved_task.completed_at
    assert isinstance(saved_task.completed_at, datetime)
    saved_task = saved_task.dict(by_alias=True)
    saved_task.pop("completedAt")
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("completedAt")
    assert saved_task == expected_task
    expected_events = [
        TaskEvent(task_id="some-id", status=TaskStatus.RUNNING, progress=0.0),
        TaskEvent(task_id="some-id", progress=0.1),
        TaskEvent(task_id="some-id", progress=0.99),
        TaskEvent(
            task_id="some-id",
            status=TaskStatus.DONE,
            progress=100.0,
            completed_at=completed_at,
        ),
    ]
    assert worker.published_events == expected_events

    expected_result = TaskResult(task_id="some-id", result="Hello world !")
    assert saved_result == expected_result


async def test_task_wrapper_run_sync_task(mock_worker: MockWorker):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, worker.db_lock, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        id="some-id",
        type="hello_world_sync",
        created_at=created_at,
        status=TaskStatus.CREATED,
        inputs={"greeted": "world"},
    )

    # When
    await task_manager.enqueue(task, project)
    await task_wrapper(worker)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)
    saved_result = await task_manager.get_task_result(task_id=task.id, project=project)
    saved_errors = await task_manager.get_task_errors(task_id=task.id, project=project)

    # Then
    assert not saved_errors

    expected_task = Task(
        id="some-id",
        type="hello_world_sync",
        progress=100,
        created_at=created_at,
        status=TaskStatus.DONE,
        inputs={"greeted": "world"},
    )
    completed_at = saved_task.completed_at
    assert isinstance(saved_task.completed_at, datetime)
    saved_task = saved_task.dict(by_alias=True)
    saved_task.pop("completedAt")
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("completedAt")
    assert saved_task == expected_task
    expected_events = [
        TaskEvent(task_id="some-id", status=TaskStatus.RUNNING, progress=0.0),
        TaskEvent(
            task_id="some-id",
            status=TaskStatus.DONE,
            progress=100.0,
            completed_at=completed_at,
        ),
    ]
    assert worker.published_events == expected_events

    expected_result = TaskResult(task_id="some-id", result="Hello world !")
    assert saved_result == expected_result


async def test_task_wrapper_should_recover_from_recoverable_error(
    mock_failing_worker: MockWorker,
):
    # Given
    worker = mock_failing_worker
    task_manager = MockManager(worker.db_path, worker.db_lock, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        id="some-id",
        type="recovering_task",
        created_at=created_at,
        status=TaskStatus.CREATED,
    )

    # When/Then
    task = await task_manager.enqueue(task, project)
    assert task.status is TaskStatus.QUEUED
    await task_wrapper(worker)
    retried_task = await task_manager.get_task(task_id=task.id, project=project)

    assert retried_task.status is TaskStatus.QUEUED
    assert retried_task.retries == 1

    await task_wrapper(worker)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)
    saved_result = await task_manager.get_task_result(task_id=task.id, project=project)
    saved_errors = await task_manager.get_task_errors(task_id=task.id, project=project)

    # Then
    expected_task = Task(
        id="some-id",
        type="recovering_task",
        progress=100,
        created_at=created_at,
        status=TaskStatus.DONE,
        retries=1,
    )
    completed_at = saved_task.completed_at
    assert isinstance(completed_at, datetime)
    saved_task = saved_task.dict(by_alias=True)
    saved_task.pop("completedAt")
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("completedAt")
    assert saved_task == expected_task

    # No error should be saved
    assert not saved_errors
    # However we expect the worker to have logged them somewhere in the events
    expected_result = TaskResult(task_id="some-id", result="i told you i could recover")
    assert saved_result == expected_result

    expected_events = [
        TaskEvent(task_id="some-id", status=TaskStatus.RUNNING, progress=0.0),
        TaskEvent(
            task_id="some-id",
            status=TaskStatus.QUEUED,
            retries=1,
            progress=None,  # The progress should be left as is waiting before retry
            error=TaskError(
                id="", title="Recoverable", detail="", occurred_at=datetime.now()
            ),
        ),
        TaskEvent(task_id="some-id", status=TaskStatus.RUNNING, progress=0.0),
        TaskEvent(task_id="some-id", progress=0.0),
        TaskEvent(
            task_id="some-id",
            status=TaskStatus.DONE,
            progress=100.0,
            completed_at=completed_at,
        ),
    ]
    events = [e.dict(by_alias=True) for e in worker.published_events]
    event_errors = [e.pop("error") for e in events]
    event_error_titles = [e["title"] if e is not None else e for e in event_errors]
    assert event_error_titles == [None, "Recoverable", None, None, None]
    event_error_occurred_at = [
        isinstance(e["occurredAt"], datetime) if e else e for e in event_errors
    ]
    assert event_error_occurred_at == [None, True, None, None, None]
    expected_events = [e.dict(by_alias=True) for e in expected_events]
    for e in expected_events:
        e.pop("error")
    assert events == expected_events


async def test_task_wrapper_should_handle_non_recoverable_error(
    mock_failing_worker: MockWorker,
):
    # Given
    worker = mock_failing_worker
    task_manager = MockManager(worker.db_path, worker.db_lock, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        id="some-id",
        type="fatal_error_task",
        created_at=created_at,
        status=TaskStatus.CREATED,
    )

    # When
    await task_manager.enqueue(task, project)
    await task_wrapper(worker)
    saved_errors = await task_manager.get_task_errors(
        task_id="some-id", project=project
    )
    saved_task = await task_manager.get_task(task_id="some-id", project=project)

    # Then
    expected_task = Task(
        id="some-id",
        type="fatal_error_task",
        progress=0.1,
        created_at=created_at,
        status=TaskStatus.ERROR,
    )
    assert saved_task == expected_task

    assert len(saved_errors) == 1
    saved_error = saved_errors[0]
    assert saved_error.title == "ValueError"
    assert isinstance(saved_error.occurred_at, datetime)

    expected_events = [
        TaskEvent(task_id="some-id", status=TaskStatus.RUNNING, progress=0.0),
        TaskEvent(task_id="some-id", progress=0.1),
        TaskEvent(
            task_id="some-id",
            status=TaskStatus.ERROR,
            error=TaskError(
                id="", title="ValueError", detail="", occurred_at=datetime.now()
            ),
        ),
    ]
    assert len(worker.published_events) == len(expected_events)
    assert worker.published_events[:-1] == expected_events[:-1]

    error_event = worker.published_events[-1]
    expected_error_event = expected_events[-1]
    assert isinstance(error_event.error, TaskError)
    assert error_event.error.title == "ValueError"
    assert isinstance(error_event.error.occurred_at, datetime)
    error_event = error_event.dict(by_alias=True)
    error_event.pop("error")
    expected_error_event = expected_error_event.dict(by_alias=True)
    expected_error_event.pop("error")
    assert error_event == expected_error_event


async def test_task_wrapper_should_handle_unregistered_task(mock_worker: MockWorker):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, worker.db_lock, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        id="some-id",
        type="i_dont_exist",
        created_at=created_at,
        status=TaskStatus.CREATED,
    )

    # When
    await task_manager.enqueue(task, project)
    await task_wrapper(worker)
    saved_task = await task_manager.get_task(task_id="some-id", project=project)
    saved_errors = await task_manager.get_task_errors(
        task_id="some-id", project=project
    )

    # Then
    expected_task = Task(
        id="some-id",
        type="i_dont_exist",
        progress=0.0,
        created_at=created_at,
        status=TaskStatus.ERROR,
    )
    assert saved_task == expected_task

    assert len(saved_errors) == 1
    saved_error = saved_errors[0]
    assert saved_error.title == "UnregisteredTask"
    assert isinstance(saved_error.occurred_at, datetime)

    expected_events = [
        TaskEvent(task_id="some-id", status=TaskStatus.RUNNING, progress=0.0),
        TaskEvent(
            task_id="some-id",
            status=TaskStatus.ERROR,
            error=TaskError(
                id="error-id",
                title="UnregisteredTask",
                detail="",
                occurred_at=datetime.now(),
            ),
        ),
    ]
    assert len(worker.published_events) == len(expected_events)
    assert worker.published_events[:-1] == expected_events[:-1]

    error_event = worker.published_events[-1]
    expected_error_event = expected_events[-1]
    assert isinstance(error_event.error, TaskError)
    assert error_event.error.title == "UnregisteredTask"
    assert isinstance(error_event.error.occurred_at, datetime)
    error_event = error_event.dict(by_alias=True)
    error_event.pop("error")
    expected_error_event = expected_error_event.dict(by_alias=True)
    expected_error_event.pop("error")
    assert error_event == expected_error_event


async def test_work_once_should_not_run_cancelled_task(mock_worker: MockWorker, caplog):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, worker.db_lock, max_queue_size=10)
    caplog.set_level(logging.INFO)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        id="some-id",
        type="fatal_error_task",
        created_at=created_at,
        status=TaskStatus.CREATED,
    )

    # When
    await task_manager.enqueue(task, project)
    await task_manager.cancel(task_id=task.id, project=project)
    with pytest.raises(TaskCancelled):
        await worker.check_cancelled(task_id=task.id, project=project, refresh=True)

    # Now we mock the fact the task is still received but cancelled right after
    with patch.object(worker, "consume", return_value=(task, project)):
        await task_wrapper(worker)
    expected = f'Task(id="{task.id}") already cancelled skipping it !'
    assert any(expected in m for m in caplog.messages)


async def test_cancel_running_task(mock_worker: MockWorker):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, worker.db_lock, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    duration = 10
    task = Task(
        id="some-id",
        type="sleep_for",
        created_at=created_at,
        status=TaskStatus.CREATED,
        inputs={"duration": duration},
    )

    # When
    asyncio_tasks = set()
    t = asyncio.create_task(task_wrapper(worker))
    t.add_done_callback(asyncio_tasks.discard)
    asyncio_tasks.add(t)

    await task_manager.enqueue(task, project)
    after_s = 2.0

    async def _assert_running() -> bool:
        saved = await task_manager.get_task(task_id=task.id, project=project)
        return saved.status is TaskStatus.RUNNING

    failure_msg = f"Failed to run task in less than {after_s}"
    assert await async_true_after(_assert_running, after_s=after_s), failure_msg
    await task_manager.cancel(task_id=task.id, project=project)

    async def _assert_cancelled() -> bool:
        saved = await task_manager.get_task(task_id=task.id, project=project)
        return saved.status is TaskStatus.CANCELLED

    failure_msg = f"Failed to cancel task in less than {after_s}"
    assert await async_true_after(_assert_cancelled, after_s=after_s), failure_msg


@pytest.mark.parametrize(
    "provided_inputs,kwargs,maybe_output",
    [
        ({}, {}, None),
        ({"a": "a"}, {}, None),
        ({"a": "a"}, {"b": "b"}, "a-b-c"),
        ({"a": "a", "b": "b"}, {"c": "not-your-average-c"}, "a-b-not-your-average-c"),
    ],
)
def test_add_missing_args(
    provided_inputs: Dict[str, Any],
    kwargs: Dict[str, Any],
    maybe_output: Optional[str],
):
    # Given
    def fn(a: str, b: str, c: str = "c") -> str:
        return f"{a}-{b}-{c}"

    # When
    all_args = add_missing_args(fn, inputs=provided_inputs, **kwargs)
    # Then
    if maybe_output is not None:
        output = fn(**all_args)
        assert output == maybe_output
    else:
        with pytest.raises(
            TypeError,
        ):
            fn(**all_args)
