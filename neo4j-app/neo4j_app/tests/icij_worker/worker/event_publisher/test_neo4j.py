# pylint: disable=redefined-outer-name
from datetime import datetime
from typing import List

import neo4j
import pytest

from neo4j_app.core.utils.pydantic import safe_copy
from neo4j_app.icij_worker import Neo4jEventPublisher, Task, TaskEvent, TaskStatus
from neo4j_app.icij_worker.task_store.neo4j import Neo4jTaskStore
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
    store = Neo4jTaskStore(publisher.driver)
    project = TEST_PROJECT
    task = populate_tasks[0]
    assert task.status == TaskStatus.CREATED
    assert task.progress is None
    assert task.retries == 0
    assert task.completed_at is None
    progress = 66.6
    status = TaskStatus.RETRY
    retries = 2
    completed_at = datetime.now()

    event = TaskEvent(
        task_id=task.id,
        progress=progress,
        retries=retries,
        status=status,
        completed_at=completed_at,
    )

    # When
    await publisher.publish_event(event=event, project=project)
    saved_task = await store.get_task(project=project, task_id=task.id)

    # Then
    update = {
        "status": status,
        "progress": progress,
        "retries": retries,
        "completed_at": completed_at,
    }
    expected = safe_copy(task, update=update)
    assert saved_task == expected


@pytest.mark.asyncio
async def test_worker_publish_event_for_unknown_task(publisher: Neo4jEventPublisher):
    # This is useful when task is not reserved yet
    # Given
    store = Neo4jTaskStore(publisher.driver)
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
    saved_task = await store.get_task(project=project, task_id=task_id)

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
    store = Neo4jTaskStore(publisher.driver)
    project = TEST_PROJECT
    task = populate_tasks[1]
    assert task.status is TaskStatus.RUNNING

    event = TaskEvent(task_id=task.id, status=TaskStatus.CREATED)

    # When
    await publisher.publish_event(event=event, project=project)
    saved_task = await store.get_task(project=project, task_id=task.id)

    # Then
    assert saved_task == task
