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
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from neo4j_app.icij_worker.task_manager.neo4j import Neo4JTaskManager
from neo4j_app.icij_worker.worker.neo4j import Neo4jAsyncWorker
from neo4j_app.tests.conftest import TEST_PROJECT, true_after


@pytest.fixture(scope="function")
def worker(test_app: ICIJApp, neo4j_app_driver: neo4j.AsyncDriver) -> Neo4jAsyncWorker:
    worker = Neo4jAsyncWorker(test_app, "test-worker", neo4j_app_driver)
    return worker


@pytest.mark.asyncio
async def test_worker_consume_task(
    worker: Neo4jAsyncWorker, populate_tasks: List[Task]
):
    # pylint: disable=unused-argument
    # Given
    registry = set()
    task = asyncio.create_task(worker.consume())
    task.add_done_callback(registry.discard)
    # Then
    true_after(task.done, after_s=1.0)


@pytest.mark.asyncio
async def test_worker_negatively_acknowledge(
    populate_tasks: List[Task], worker: Neo4jAsyncWorker
):
    # pylint: disable=unused-argument
    # When
    task, project = await worker.consume()
    nacked = await worker.negatively_acknowledge(task, project, requeue=False)

    # Then
    update = {"status": TaskStatus.ERROR}
    expected_nacked = safe_copy(task, update=update)
    assert nacked == expected_nacked
    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    recs, _, _ = await worker.driver.execute_query(count_locks_query)
    assert recs[0]["nLocks"] == 0


@pytest.mark.asyncio
async def test_worker_negatively_acknowledge_and_requeue(
    populate_tasks: List[Task], worker: Neo4jAsyncWorker
):
    # pylint: disable=unused-argument
    # Given
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        id="some-id",
        type="hello_world",
        created_at=created_at,
        status=TaskStatus.CREATED,
    )

    # When
    await task_manager.enqueue(task, project)
    task, project = await worker.consume()
    # Let's publish some event to increment the progress and check that it's reset
    # correctly to 0
    event = TaskEvent(task_id=task.id, progress=50.0)
    await worker.publish_event(event, project)
    with_progress = safe_copy(task, update={"progress": event.progress})
    nacked = await worker.negatively_acknowledge(task, project, requeue=True)

    # Then
    update = {"status": TaskStatus.QUEUED, "progress": 0.0}
    expected_nacked = safe_copy(with_progress, update=update)
    assert nacked == expected_nacked
    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    recs, _, _ = await worker.driver.execute_query(count_locks_query)
    assert recs[0]["nLocks"] == 0


@pytest.mark.asyncio
async def test_worker_save_result(populate_tasks: List[Task], worker: Neo4jAsyncWorker):
    # Given
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    task = populate_tasks[0]
    assert task.status == TaskStatus.QUEUED
    result = "hello everyone"
    task_result = TaskResult(task_id=task.id, result=result)

    # When
    await worker.save_result(result=task_result, project=project)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)
    saved_result = await task_manager.get_task_result(task_id=task.id, project=project)

    # Then
    assert saved_task == task
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

    # When
    await worker.save_result(result=task_result, project=project)
    # Then
    expected = "Attempted to save result for task task-0 but found existing result"
    with pytest.raises(ValueError, match=expected):
        await worker.save_result(result=task_result, project=project)


@pytest.mark.asyncio
@pytest.mark.parametrize("retries,expected_retries", [(None, 0), (3, 3)])
async def test_worker_save_error(
    populate_tasks: List[Task],
    worker: Neo4jAsyncWorker,
    retries: Optional[int],
    expected_retries: int,
):
    # Given
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    error = TaskError(
        id="error-id",
        title="someErrorTitle",
        detail="with_details",
        occurred_at=datetime.now(),
    )

    # When
    task, _ = await worker.consume()
    await worker.save_error(error=error, task=task, project=project, retries=retries)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)
    saved_errors = await task_manager.get_task_errors(task_id=task.id, project=project)

    # Then
    # We don't expect the task status to be updated by saving the error, the negative
    # acknowledgment will do it
    update = {"retries": expected_retries}
    expected = safe_copy(task, update=update)
    assert saved_task == expected
    assert saved_errors == [error]


@pytest.mark.asyncio
async def test_worker_acknowledgment_cm(
    populate_tasks: List[Task], worker: Neo4jAsyncWorker
):
    # Given
    created = populate_tasks[0]
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT

    # When
    async with worker.acknowledgment_cm(created, project):
        await worker.consume()
        task = await task_manager.get_task(task_id=created.id, project=TEST_PROJECT)
        assert task.status is TaskStatus.RUNNING

    # Then
    task = await task_manager.get_task(task_id=created.id, project=TEST_PROJECT)
    update = {"progress": 100.0, "status": TaskStatus.DONE}
    expected_task = safe_copy(task, update=update).dict(by_alias=True)
    expected_task.pop("completedAt")
    assert task.completed_at is not None
    task = task.dict(by_alias=True)
    task.pop("completedAt")
    assert task == expected_task
    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    recs, _, _ = await worker.driver.execute_query(count_locks_query)
    assert recs[0]["nLocks"] == 0
