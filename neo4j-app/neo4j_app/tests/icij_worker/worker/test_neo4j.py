# pylint: disable=redefined-outer-name
import asyncio
from datetime import datetime
from typing import List, Optional

import neo4j
import pytest

from neo4j_app.core.utils.pydantic import safe_copy
from neo4j_app.icij_worker import (
    ICIJApp,
    Task,
    TaskError,
    TaskResult,
    TaskStatus,
)
from neo4j_app.icij_worker.exceptions import TaskAlreadyReserved, UnregisteredTask
from neo4j_app.icij_worker.task_store.neo4j import Neo4jTaskStore
from neo4j_app.icij_worker.worker.neo4j import Neo4jAsyncWorker
from neo4j_app.tests.conftest import TEST_PROJECT, true_after


@pytest.fixture(scope="function")
def worker(test_app: ICIJApp, neo4j_app_driver: neo4j.AsyncDriver) -> Neo4jAsyncWorker:
    worker = Neo4jAsyncWorker(test_app, "test-worker", neo4j_app_driver)
    return worker


@pytest.mark.asyncio
async def test_worker_receive_task(
    worker: Neo4jAsyncWorker, populate_tasks: List[Task]
):
    # pylint: disable=unused-argument
    # Given
    registry = set()
    task = asyncio.create_task(worker.receive())
    task.add_done_callback(registry.discard)
    # Then
    true_after(task.done, after_s=1.0)


@pytest.mark.asyncio
async def test_worker_lock_should_raise_when_task_already_locked(
    populate_tasks: List[Task], worker: Neo4jAsyncWorker
):
    # Given
    created = populate_tasks[0]
    # When
    async with worker.lock(created, TEST_PROJECT):
        with pytest.raises(TaskAlreadyReserved):
            async with worker.lock(created, TEST_PROJECT):
                pass


@pytest.mark.asyncio
async def test_worker_save_result(populate_tasks: List[Task], worker: Neo4jAsyncWorker):
    # Given
    store = Neo4jTaskStore(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    task = populate_tasks[0]
    assert task.status == TaskStatus.QUEUED
    result = "hello everyone"
    task_result = TaskResult(task_id=task.id, result=result)
    completed_at = datetime.now()

    # When
    await worker.save_result(
        result=task_result, project=project, completed_at=completed_at
    )
    saved_task = await store.get_task(task_id=task.id, project=project)
    saved_result = await store.get_task_result(task_id=task.id, project=project)

    # Then
    update = {"status": TaskStatus.DONE, "progress": 100.0}
    expected_task = safe_copy(task, update=update)
    assert saved_task == expected_task

    assert saved_result == task_result


@pytest.mark.asyncio
async def test_worker_should_raise_when_saving_existing_result(
    populate_tasks: List[Task], worker: Neo4jAsyncWorker
):
    # Given
    project = TEST_PROJECT
    task = populate_tasks[0]
    assert task.status == TaskStatus.QUEUED
    result = "hello everyone"
    task_result = TaskResult(task_id=task.id, result=result)
    completed_at = datetime.now()

    # When
    await worker.save_result(
        result=task_result, project=project, completed_at=completed_at
    )
    # Then
    expected = "Attempted to save result for task task-0 but found existing result"
    with pytest.raises(ValueError, match=expected):
        await worker.save_result(
            result=task_result, project=project, completed_at=completed_at
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "retries,expected_status,expected_retries",
    [(None, TaskStatus.ERROR, 0), (3, TaskStatus.RETRY, 3)],
)
async def test_worker_save_error(
    populate_tasks: List[Task],
    worker: Neo4jAsyncWorker,
    retries: Optional[int],
    expected_status: TaskStatus,
    expected_retries: int,
):
    # Given
    store = Neo4jTaskStore(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    task = populate_tasks[0]
    error = TaskError(
        id="error-id",
        title="someErrorTitle",
        detail="with_details",
        occurred_at=datetime.now(),
    )

    # When
    await worker.save_error(error=error, task=task, project=project, retries=retries)
    saved_task = await store.get_task(task_id=task.id, project=project)
    saved_errors = await store.get_task_errors(task_id=task.id, project=project)

    # Then
    update = {"status": expected_status, "retries": expected_retries}
    expected = safe_copy(task, update=update)
    assert saved_task == expected
    assert saved_errors == [error]


@pytest.mark.asyncio
async def test_worker_should_save_error_for_unknown_task(
    populate_tasks, worker: Neo4jAsyncWorker  # pylint: disable=unused-argument
):
    # This is useful when the error occurs before it's reserved by a worker, we want to
    # make sure the error is correctly saved even if the task is not valid
    # Given
    store = Neo4jTaskStore(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    unregistered_task = Task(
        id="task_id",
        type="not_registered",
        created_at=created_at,
        status=TaskStatus.CREATED,
    )
    occurred_at = datetime.now()
    error = TaskError(
        id="error-id",
        title=UnregisteredTask.__class__.__name__,
        detail="I failed typically because I'm not registered so the worker failed to"
        " reserve me. That being said I still deserved to be correctly saved",
        occurred_at=occurred_at,
    )

    # When
    await worker.save_error(error, unregistered_task, project)
    saved_task = await store.get_task(task_id=unregistered_task.id, project=project)
    saved_errors = await store.get_task_errors(
        task_id=unregistered_task.id, project=project
    )

    # Then
    update = {"status": TaskStatus.ERROR}
    expected_task = safe_copy(unregistered_task, update=update)
    assert saved_task == expected_task
    assert saved_errors == [error]


@pytest.mark.asyncio
async def test_worker_acknowledge(populate_tasks: List[Task], worker: Neo4jAsyncWorker):
    # Given
    created = populate_tasks[0]
    store = Neo4jTaskStore(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    completed_at = datetime.now()

    # When
    async with worker.lock(created, project):
        task = await store.get_task(task_id=created.id, project=TEST_PROJECT)
        assert task.status is TaskStatus.RUNNING
        await worker.acknowledge(created, project, completed_at=completed_at)

    # Then
    task = await store.get_task(task_id=created.id, project=TEST_PROJECT)
    update = {
        "progress": 100.0,
        "status": TaskStatus.DONE,
        "completed_at": completed_at,
    }
    expected_task = safe_copy(task, update=update).dict(by_alias=True)
    assert task.completed_at is not None
    task = task.dict(by_alias=True)
    assert task == expected_task
