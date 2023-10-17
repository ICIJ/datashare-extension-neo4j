import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from functools import cached_property
from typing import AsyncGenerator, Dict, List, Optional, Tuple

import neo4j
from neo4j.exceptions import ConstraintError, ResultNotSingleError

from neo4j_app.constants import (
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_HAS_RESULT_TYPE,
    TASK_ID,
    TASK_LOCK_NODE,
    TASK_LOCK_TASK_ID,
    TASK_LOCK_WORKER_ID,
    TASK_NODE,
    TASK_RESULT_NODE,
    TASK_RESULT_RESULT,
)
from neo4j_app.core.neo4j.migrations.migrate import retrieve_projects
from neo4j_app.core.neo4j.projects import project_db_session
from neo4j_app.icij_worker import (
    ICIJApp,
    Task,
    TaskError,
    TaskResult,
    TaskStatus,
)
from neo4j_app.icij_worker.event_publisher.neo4j import Neo4jEventPublisher
from neo4j_app.icij_worker.exceptions import TaskAlreadyReserved, UnknownTask
from neo4j_app.icij_worker.worker.process import ProcessWorkerMixin

_TASK_MANDATORY_FIELDS_BY_ALIAS = {
    f for f in Task.schema(by_alias=True)["required"] if f != "id"
}


class Neo4jAsyncWorker(ProcessWorkerMixin, Neo4jEventPublisher):
    def __init__(
        self,
        app: ICIJApp,
        worker_id: str,
        driver: Optional[neo4j.AsyncDriver] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(app, worker_id)
        self._inherited_driver = False
        if driver is None:
            self._inherited_driver = True
            driver = app.config.to_neo4j_driver()
        Neo4jEventPublisher.__init__(self, driver)
        if logger is None:
            logger = logging.getLogger(__name__)
        self._logger_ = logger

    @cached_property
    def logged_named(self) -> str:
        return super().logged_named

    async def _receive(self) -> Tuple[Task, str]:
        projects = await retrieve_projects(self._driver)
        while "waiting for some task to be available for some project":
            for p in projects:
                async with self._project_session(p.name) as sess:
                    received = await sess.execute_read(_receive_task_tx)
                    if received is not None:
                        return received, p.name
            await asyncio.sleep(self._app.config.neo4j_app_task_queue_poll_interval_s)

    async def _lock(self, task: Task, project: str):
        async with self._project_session(project) as sess:
            await sess.execute_write(_lock_task_tx, task_id=task.id, worker_id=self.id)

    async def _unlock(self, task: Task, project: str):
        async with self._project_session(project) as sess:
            await sess.execute_write(
                _unlock_task_tx, task_id=task.id, worker_id=self.id
            )

    async def _save_result(self, result: TaskResult, project: str):
        async with self._project_session(project) as sess:
            res_str = json.dumps(result.result)
            await sess.execute_write(
                _save_result_tx, task_id=result.task_id, result=res_str
            )

    async def _save_error(self, error: TaskError, task: Task, project: str):
        async with self._project_session(project) as sess:
            task_props = {
                k: v
                for k, v in task.dict(by_alias=True).items()
                if k in _TASK_MANDATORY_FIELDS_BY_ALIAS
            }
            task_props.pop("status")
            await sess.execute_write(
                _save_error_tx,
                task_id=task.id,
                task_props=task_props,
                error_props=error.dict(by_alias=True),
            )

    async def acknowledge(self, task: Task, project: str, completed_at: datetime):
        async with self._project_session(project) as sess:
            await sess.execute_write(
                _acknowledge_task_tx,
                task_id=task.id,
                worker_id=self.id,
                completed_at=completed_at,
            )

    async def _refresh_cancelled(self, project: str):
        async with self._project_session(project) as sess:
            self._cancelled_[project] = await sess.execute_read(_cancelled_task_tx)

    @asynccontextmanager
    async def _project_session(
        self, project: str
    ) -> AsyncGenerator[neo4j.AsyncSession, None]:
        async with project_db_session(self._driver, project) as sess:
            yield sess

    @property
    def _logger(self) -> logging.Logger:
        return self._logger_

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._inherited_driver:
            await self._driver.__aexit__(exc_type, exc_val, exc_tb)


async def _receive_task_tx(tx: neo4j.AsyncTransaction) -> Optional[Task]:
    query = f"""MATCH (task:{TASK_NODE}:`{TaskStatus.QUEUED.value}`)
RETURN task
ORDER BY task.createdAt ASC 
LIMIT 1"""
    res = await tx.run(query)
    try:
        task = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    return Task.from_neo4j(task)


async def _lock_task_tx(tx: neo4j.AsyncTransaction, task_id: str, worker_id: str):
    query = f"""CREATE (lock:{TASK_LOCK_NODE} {{
    {TASK_LOCK_TASK_ID}: $taskId,
    {TASK_LOCK_WORKER_ID}: $workerId 
}})
WITH lock
MATCH (t:{TASK_NODE}:`{TaskStatus.QUEUED.value}` {{ {TASK_ID}: $taskId }})
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
RETURN task, lock"""
    labels = [TASK_NODE, TaskStatus.RUNNING.value]
    res = await tx.run(query, taskId=task_id, workerId=worker_id, labels=labels)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id=task_id) from e
    except ConstraintError as e:
        raise TaskAlreadyReserved(task_id=task_id) from e


async def _acknowledge_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str, completed_at: datetime
):
    query = f"""MATCH (t:{TASK_NODE} {{ {TASK_ID}: $taskId }})
SET t.progress = 100.0, t.completedAt = $completedAt
WITH t 
CALL apoc.create.setLabels(t, $labels) YIELD node as task
RETURN task"""
    labels = [TASK_NODE, TaskStatus.DONE.value]
    res = await tx.run(
        query,
        taskId=task_id,
        workerId=worker_id,
        labels=labels,
        completedAt=completed_at,
    )
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


async def _unlock_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str):
    query = f"""MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: $taskId }})
WHERE lock.{TASK_LOCK_WORKER_ID} = $workerId
DELETE lock
RETURN lock"""
    res = await tx.run(query, taskId=task_id, workerId=worker_id)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


async def _cancelled_task_tx(tx: neo4j.AsyncTransaction) -> List[str]:
    query = f"""MATCH (task:{TASK_NODE}:`{TaskStatus.CANCELLED.value}`)
RETURN task.id as taskId"""
    res = await tx.run(query)
    return [rec["taskId"] async for rec in res]


async def _save_result_tx(tx: neo4j.AsyncTransaction, *, task_id: str, result: str):
    query = f"""MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
MERGE (task)-[:{TASK_HAS_RESULT_TYPE}]->(result:{TASK_RESULT_NODE})
ON CREATE SET result.{TASK_RESULT_RESULT} = $result
RETURN task, result"""
    res = await tx.run(query, taskId=task_id, result=result)
    records = [rec async for rec in res]
    summary = await res.consume()
    if not records:
        raise UnknownTask(task_id)
    if not summary.counters.relationships_created:
        msg = f"Attempted to save result for task {task_id} but found existing result"
        raise ValueError(msg)


async def _save_error_tx(
    tx: neo4j.AsyncTransaction, task_id: str, *, task_props: Dict, error_props: Dict
):
    query = f"""MERGE (t:{TASK_NODE} {{{TASK_ID}: $taskId }})
ON CREATE SET t += $taskProps, t:`{TaskStatus.ERROR.value}` 
CREATE (error:{TASK_ERROR_NODE} $errorProps)-[:{TASK_ERROR_OCCURRED_TYPE}]->(task)
RETURN task, error"""
    res = await tx.run(
        query,
        taskId=task_id,
        taskProps=task_props,
        errorProps=error_props,
    )
    records = [rec async for rec in res]
    if not records:
        raise UnknownTask(task_id)
