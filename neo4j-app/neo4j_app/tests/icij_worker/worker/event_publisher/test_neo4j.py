# pylint: disable=redefined-outer-name
from datetime import datetime
from typing import List

import neo4j
import pytest

from neo4j_app.core.utils.pydantic import safe_copy
from neo4j_app.icij_worker import Neo4jEventPublisher, Task, TaskEvent, TaskStatus
from neo4j_app.icij_worker.task_manager.neo4j import Neo4JTaskManager
from neo4j_app.tests.conftest import TEST_PROJECT


@pytest.fixture(scope="function")
def publisher(neo4j_app_driver: neo4j.AsyncDriver) -> Neo4jEventPublisher:
    worker = Neo4jEventPublisher(neo4j_app_driver)
    return worker


@pytest.mark.asyncio
async def test_worker_publish_event(
    populate_tasks: List[Task], publisher: Neo4jEventPublisher
):
    # Given
    task_manager = Neo4JTaskManager(publisher.driver, max_queue_size=10)
    project = TEST_PROJECT
    task = populate_tasks[0]
    assert task.status == TaskStatus.QUEUED
    assert task.progress is None
    assert task.retries == 0
    assert task.completed_at is None
    progress = 66.6
    status = TaskStatus.RETRY
    retries = 2

    event = TaskEvent(
        task_id=task.id, progress=progress, retries=retries, status=status
    )

    # When
    await publisher.publish_event(event=event, project=project)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)

    # Then
    # Status is not updated by event
    update = {"progress": progress, "retries": retries}
    expected = safe_copy(task, update=update)
    assert saved_task == expected


@pytest.mark.asyncio
async def test_worker_publish_done_task_event_should_not_update_task(
    publisher: Neo4jEventPublisher,
):
    # Given
    project = TEST_PROJECT
    query = """CREATE (task:_Task:DONE {
        id: 'task-0', 
        type: 'hello_world',
        createdAt: $now,
        completedAt: $now,
        inputs: '{"greeted": "0"}'
     }) 
    RETURN task"""
    async with publisher.driver.session() as sess:
        res = await sess.run(query, now=datetime.now())
        completed = await res.single()
    completed = Task.from_neo4j(completed)
    task_manager = Neo4JTaskManager(publisher.driver, max_queue_size=10)
    event = TaskEvent(
        task_id=completed.id,
        progress=0.99,
        status=TaskStatus.RUNNING,
        completed_at=datetime.now(),
    )

    # When
    await publisher.publish_event(event=event, project=project)
    saved_task = await task_manager.get_task(task_id=completed.id, project=project)

    # Then
    assert saved_task == completed


@pytest.mark.asyncio
async def test_worker_publish_event_for_unknown_task(publisher: Neo4jEventPublisher):
    # This is useful when task is not reserved yet
    # Given
    task_manager = Neo4JTaskManager(publisher.driver, max_queue_size=10)
    project = TEST_PROJECT

    task_id = "some-id"
    task_type = "hello_world"
    created_at = datetime.now()
    event = TaskEvent(
        task_id=task_id,
        task_type=task_type,
        created_at=created_at,
        status=TaskStatus.QUEUED,
    )

    # When
    await publisher.publish_event(event=event, project=project)
    saved_task = await task_manager.get_task(task_id=task_id, project=project)

    # Then
    expected = Task(
        id=task_id, type=task_type, created_at=created_at, status=TaskStatus.QUEUED
    )
    assert saved_task == expected


@pytest.mark.asyncio
async def test_worker_publish_event_should_use_status_resolution(
    populate_tasks: List[Task], publisher: Neo4jEventPublisher
):
    # Given
    task_manager = Neo4JTaskManager(publisher.driver, max_queue_size=10)
    project = TEST_PROJECT
    task = populate_tasks[1]
    assert task.status is TaskStatus.RUNNING

    event = TaskEvent(task_id=task.id, status=TaskStatus.CREATED)

    # When
    await publisher.publish_event(event=event, project=project)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)

    # Then
    assert saved_task == task
