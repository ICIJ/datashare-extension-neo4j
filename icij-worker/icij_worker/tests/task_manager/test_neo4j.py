from datetime import datetime
from typing import List, Optional, Tuple

import neo4j
import pytest
import pytest_asyncio
from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import TEST_PROJECT

from icij_worker import Task, TaskError, TaskResult, TaskStatus
from icij_worker.exceptions import MissingTaskResult, TaskAlreadyExists, TaskQueueIsFull
from icij_worker.task_manager.neo4j import Neo4JTaskManager


@pytest_asyncio.fixture(scope="function")
async def _populate_errors(
    populate_tasks: List[Task], neo4j_async_app_driver: neo4j.AsyncDriver
) -> List[Tuple[Task, List[TaskError]]]:
    task_with_error = populate_tasks[1]
    query_0 = """MATCH (task:_Task { id: $taskId })
CREATE  (error:_TaskError {
    id: 'error-0',
    title: 'error',
    detail: 'with details',
    occurredAt: $now 
})-[:_OCCURRED_DURING]->(task)
RETURN error"""
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_0, taskId=task_with_error.id, now=datetime.now()
    )
    e_0 = TaskError.from_neo4j(recs_0[0])
    query_1 = """MATCH (task:_Task { id: $taskId })
CREATE  (error:_TaskError {
    id: 'error-1',
    title: 'error',
    detail: 'same error again',
    occurredAt: $now 
})-[:_OCCURRED_DURING]->(task)
RETURN error"""
    recs_1, _, _ = await neo4j_async_app_driver.execute_query(
        query_1,
        taskId=task_with_error.id,
        now=datetime.now(),
    )
    e_1 = TaskError.from_neo4j(recs_1[0])
    return list(zip(populate_tasks, [[], [e_0, e_1]]))


@pytest_asyncio.fixture(scope="function")
async def _populate_results(
    populate_tasks: List[Task], neo4j_async_app_driver: neo4j.AsyncDriver
) -> List[Tuple[Task, List[TaskResult]]]:
    query_1 = """CREATE (task:_Task:DONE {
    id: 'task-2', 
    type: 'hello_world',
    createdAt: $now,
    completedAt: $after,
    inputs: '{"greeted": "2"}'
})
CREATE (result:_TaskResult { result: '"Hello 2"' })
CREATE (task)-[:_HAS_RESULT]->(result)
RETURN task, result"""
    now = datetime.now()
    after = datetime.now()
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_1, now=now, after=after
    )
    t_2 = Task.from_neo4j(recs_0[0])
    r_2 = TaskResult.from_neo4j(recs_0[0])
    tasks = populate_tasks + [t_2]
    return list(zip(tasks, [None, None, r_2]))


async def test_task_manager_get_task(
    neo4j_async_app_driver: neo4j.AsyncDriver, populate_tasks: List[Task]
):
    # Given
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=10)
    project = TEST_PROJECT
    second_task = populate_tasks[1]

    # When
    task = await task_manager.get_task(task_id=second_task.id, project=project)
    task = task.dict(by_alias=True)

    # Then
    expected_task = Task(
        id="task-1",
        type="hello_world",
        inputs={"greeted": "1"},
        status=TaskStatus.RUNNING,
        progress=66.6,
        created_at=datetime.now(),
        retries=1,
    )
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("createdAt")

    assert task.pop("createdAt")  # We just check that it's not None
    assert task == expected_task


async def test_task_manager_get_completed_task(
    neo4j_async_app_driver: neo4j.AsyncDriver,
    _populate_results: List[Tuple[Task, List[TaskResult]]],
):
    # pylint: disable=invalid-name
    # Given
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=10)
    project = TEST_PROJECT
    last_task = _populate_results[-1][0]

    # When
    task = await task_manager.get_task(task_id=last_task.id, project=project)

    # Then
    assert isinstance(task.completed_at, datetime)


@pytest.mark.parametrize(
    "statuses,task_type,expected_ix",
    [
        (None, None, [0, 1]),
        ([], None, [0, 1]),
        (None, "hello_word", []),
        (None, "i_dont_exists", []),
        (TaskStatus.QUEUED, None, [0]),
        ([TaskStatus.QUEUED], None, [0]),
        (TaskStatus.RUNNING, None, [1]),
        (TaskStatus.CANCELLED, None, []),
    ],
)
async def test_task_manager_get_tasks(
    neo4j_async_app_driver: neo4j.AsyncDriver,
    populate_tasks: List[Task],
    statuses: Optional[List[TaskStatus]],
    task_type: Optional[str],
    expected_ix: List[int],
):
    # Given
    project = TEST_PROJECT
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=10)

    # When
    tasks = await task_manager.get_tasks(
        project=project, status=statuses, task_type=task_type
    )
    tasks = sorted(tasks, key=lambda t: t.id)

    # Then
    expected_tasks = [populate_tasks[i] for i in expected_ix]
    assert tasks == expected_tasks


@pytest.mark.parametrize(
    "task_id,expected_errors",
    [
        ("task-0", []),
        (
            "task-1",
            [
                TaskError(
                    id="error-0",
                    title="error",
                    detail="with details",
                    occurred_at=datetime.now(),
                ),
                TaskError(
                    id="error-1",
                    title="error",
                    detail="same error again",
                    occurred_at=datetime.now(),
                ),
            ],
        ),
    ],
)
async def test_get_task_errors(
    neo4j_async_app_driver: neo4j.AsyncDriver,
    _populate_errors: List[Tuple[Task, List[TaskError]]],
    task_id: str,
    expected_errors: List[TaskError],
):
    # pylint: disable=invalid-name
    # Given
    project = TEST_PROJECT
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=10)

    # When
    retrieved_errors = await task_manager.get_task_errors(
        task_id=task_id, project=project
    )

    # Then
    retrieved_errors = [e.dict(by_alias=True) for e in retrieved_errors]
    assert all(e["occurredAt"] for e in retrieved_errors)
    for e in retrieved_errors:
        e.pop("occurredAt")
    expected_errors = [e.dict(by_alias=True) for e in expected_errors[::-1]]
    for e in expected_errors:
        e.pop("occurredAt")
    assert retrieved_errors == expected_errors


@pytest.mark.parametrize(
    "task_id,expected_result",
    [
        ("task-0", None),
        ("task-1", None),
        ("task-2", TaskResult(task_id="task-2", result="Hello 2")),
    ],
)
async def test_task_manager_get_task_result(
    neo4j_async_app_driver: neo4j.AsyncDriver,
    _populate_results: List[Tuple[str, Optional[TaskResult]]],
    task_id: str,
    expected_result: Optional[TaskResult],
):
    # pylint: disable=invalid-name
    # Given
    project = TEST_PROJECT
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=10)

    # When/ Then
    if expected_result is None:
        expected_msg = (
            f'Result of task "{task_id}" couldn\'t be found, did it complete ?'
        )
        with pytest.raises(MissingTaskResult, match=expected_msg):
            await task_manager.get_task_result(task_id=task_id, project=project)
    else:
        result = await task_manager.get_task_result(task_id=task_id, project=project)
        assert result == expected_result


async def test_task_manager_enqueue(neo4j_async_app_driver: neo4j.AsyncDriver):
    # Given
    project = TEST_PROJECT
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=10)
    task = Task(
        id="some-id",
        type="hello_world",
        status=TaskStatus.CREATED,
        created_at=datetime.now(),
        inputs={"greeted": "world"},
    )

    # When
    queued = await task_manager.enqueue(task, project)

    # Then
    update = {"status": TaskStatus.QUEUED}
    expected = safe_copy(task, update=update)
    assert queued == expected


async def test_task_manager_enqueue_should_raise_for_existing_task(
    neo4j_async_app_driver: neo4j.AsyncDriver,
):
    # Given
    project = TEST_PROJECT
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=10)
    task = Task(
        id="some-id",
        type="hello_world",
        status=TaskStatus.CREATED,
        created_at=datetime.now(),
        inputs={"greeted": "world"},
    )
    await task_manager.enqueue(task, project)

    # When/Then
    with pytest.raises(TaskAlreadyExists):
        await task_manager.enqueue(task, project)


async def test_task_manager_cancel(neo4j_async_app_driver: neo4j.AsyncDriver):
    # Given
    project = TEST_PROJECT
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=10)
    task = Task(
        id="some-id", type="hello", status=TaskStatus.CREATED, created_at=datetime.now()
    )

    # When
    task = await task_manager.enqueue(task, project)
    task = await task_manager.cancel(task_id=task.id, project=project)

    # Then
    update = {"status": TaskStatus.CANCELLED}
    expected = safe_copy(task, update=update)
    assert task == expected


async def test_task_manager_enqueue_should_raise_when_queue_full(
    neo4j_async_app_driver: neo4j.AsyncDriver,
):
    project = TEST_PROJECT
    task_manager = Neo4JTaskManager(neo4j_async_app_driver, max_queue_size=-1)
    task = Task(
        id="some-id", type="hello", status=TaskStatus.CREATED, created_at=datetime.now()
    )

    # When
    with pytest.raises(TaskQueueIsFull):
        await task_manager.enqueue(task, project)
