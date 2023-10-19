import itertools
import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, List, Optional, Union

import neo4j

from neo4j_app.constants import (
    TASK_CREATED_AT,
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_AT,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_HAS_RESULT_TYPE,
    TASK_ID,
    TASK_INPUTS,
    TASK_NODE,
    TASK_RESULT_NODE,
    TASK_TYPE,
)
from neo4j_app.core.neo4j.projects import project_db_session
from neo4j_app.icij_worker.exceptions import (
    MissingTaskResult,
    TaskQueueIsFull,
    UnknownTask,
)
from neo4j_app.icij_worker.task import Task, TaskError, TaskResult, TaskStatus
from neo4j_app.icij_worker.task_manager import TaskManager


class Neo4JTaskManager(TaskManager):
    def __init__(self, driver: neo4j.AsyncDriver, max_queue_size: int):
        self._driver = driver
        self._max_queue_size = max_queue_size

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver

    async def get_task(self, *, task_id: str, project: str) -> Task:
        async with project_db_session(self._driver, project) as sess:
            return await sess.execute_read(_get_task_tx, task_id=task_id)

    async def get_task_errors(self, task_id: str, project: str) -> List[TaskError]:
        async with project_db_session(self._driver, project) as sess:
            return await sess.execute_read(_get_task_errors_tx, task_id=task_id)

    async def get_task_result(self, task_id: str, project: str) -> TaskResult:
        async with project_db_session(self._driver, project) as sess:
            return await sess.execute_read(_get_task_result_tx, task_id=task_id)

    async def get_tasks(
        self,
        project: str,
        task_type: Optional[str] = None,
        status: Optional[Union[List[TaskStatus], TaskStatus]] = None,
    ) -> List[Task]:
        async with project_db_session(self._driver, project) as sess:
            return await _get_tasks(sess, status=status, task_type=task_type)

    async def _enqueue(self, task: Task, project: str) -> Task:
        async with project_db_session(self._driver, project) as sess:
            inputs = json.dumps(task.inputs)
            return await sess.execute_write(
                _enqueue_task_tx,
                task_id=task.id,
                task_type=task.type,
                created_at=task.created_at,
                max_queue_size=self._max_queue_size,
                inputs=inputs,
            )

    async def _cancel(self, *, task_id: str, project: str) -> Task:
        async with project_db_session(self._driver, project) as sess:
            return await sess.execute_write(_cancel_task_tx, task_id=task_id)

    @asynccontextmanager
    async def _project_session(
        self, project: str
    ) -> AsyncGenerator[neo4j.AsyncSession, None]:
        async with project_db_session(self._driver, project) as sess:
            yield sess


async def _get_tasks(
    sess: neo4j.AsyncSession,
    status: Optional[Union[List[TaskStatus], TaskStatus]],
    task_type: Optional[str],
) -> List[Task]:
    if isinstance(status, TaskStatus):
        status = [status]
    if status is not None:
        status = [s.value for s in status]
    return await sess.execute_read(_get_tasks_tx, status=status, task_type=task_type)


async def _get_task_tx(tx: neo4j.AsyncTransaction, task_id: str) -> Task:
    query = f"MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }}) RETURN task"
    res = await tx.run(query, taskId=task_id)
    tasks = [Task.from_neo4j(t) async for t in res]
    if not tasks:
        raise UnknownTask(task_id)
    return tasks[0]


async def _get_tasks_tx(
    tx: neo4j.AsyncTransaction, status: Optional[List[str]], task_type: Optional[str]
) -> List[Task]:
    where = ""
    if task_type:
        where = f"WHERE task.{TASK_TYPE} = $type"
    all_labels = [(TASK_NODE,)]
    if isinstance(status, str):
        status = (status,)
    if status is not None:
        all_labels.append(tuple(status))
    all_labels = list(itertools.product(*all_labels))
    if all_labels:
        query = "UNION\n".join(
            f"MATCH (task:{':'.join(labels)}) {where} RETURN task"
            for labels in all_labels
        )
    else:
        query = f"MATCH (task:{TASK_NODE}) RETURN task"
    res = await tx.run(query, status=status, type=task_type)
    tasks = [Task.from_neo4j(t) async for t in res]
    return tasks


async def _get_task_errors_tx(
    tx: neo4j.AsyncTransaction, task_id: str
) -> List[TaskError]:
    query = f"""MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
MATCH (error:{TASK_ERROR_NODE})-[:{TASK_ERROR_OCCURRED_TYPE}]->(task)
RETURN error
ORDER BY error.{TASK_ERROR_OCCURRED_AT} DESC
"""
    res = await tx.run(query, taskId=task_id)
    errors = [TaskError.from_neo4j(t) async for t in res]
    return errors


async def _get_task_result_tx(tx: neo4j.AsyncTransaction, task_id: str) -> TaskResult:
    query = f"""MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
MATCH (task)-[:{TASK_HAS_RESULT_TYPE}]->(result:{TASK_RESULT_NODE})
RETURN task, result
"""
    res = await tx.run(query, taskId=task_id)
    results = [TaskResult.from_neo4j(t) async for t in res]
    if not results:
        raise MissingTaskResult(task_id)
    return results[0]


async def _enqueue_task_tx(
    tx: neo4j.AsyncTransaction,
    *,
    task_id: str,
    task_type: str,
    created_at: datetime,
    inputs: str,
    max_queue_size: int,
) -> Task:
    count_query = f"""MATCH (task:{TASK_NODE}:`{TaskStatus.QUEUED.value}`)
RETURN count(task.id) AS nQueued
"""
    res = await tx.run(count_query)
    count = await res.single(strict=True)
    n_queued = count["nQueued"]
    if n_queued > max_queue_size:
        raise TaskQueueIsFull(max_queue_size)

    query = f"""CREATE (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
SET task:{TaskStatus.QUEUED.value},
    task.{TASK_TYPE} = $taskType,
    task.{TASK_INPUTS} = $inputs,
    task.{TASK_CREATED_AT} = $createdAt 
RETURN task
"""
    res = await tx.run(
        query, taskId=task_id, taskType=task_type, createdAt=created_at, inputs=inputs
    )
    task = await res.single(strict=True)
    return Task.from_neo4j(task)


async def _cancel_task_tx(tx: neo4j.AsyncTransaction, task_id: str):
    query = f"""MATCH (t:{TASK_NODE} {{ {TASK_ID}: $taskId }})
CALL apoc.create.setLabels(t, $labels) YIELD node as task
RETURN task
"""
    labels = [TASK_NODE, TaskStatus.CANCELLED.value]
    res = await tx.run(query, taskId=task_id, labels=labels)
    task = await res.single(strict=True)
    return Task.from_neo4j(task)
