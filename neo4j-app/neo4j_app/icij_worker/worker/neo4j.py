import json
import logging
from contextlib import asynccontextmanager
from functools import cached_property
from multiprocessing import Queue
from typing import AsyncGenerator, Dict, Optional

import neo4j

from neo4j_app.constants import (
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_HAS_RESULT_TYPE,
    TASK_ID,
    TASK_NODE,
    TASK_RESULT_NODE,
    TASK_RESULT_RESULT,
)
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
    from_config = ProcessWorkerMixin.from_config

    def __init__(
        self,
        app: ICIJApp,
        worker_id: str,
        queue: Queue,
        driver: Optional[neo4j.AsyncDriver] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(app, worker_id, queue)
        self._inherited_driver = False
        if driver is None:
            self._inherited_driver = True
            driver = app.config.to_neo4j_driver()
        Neo4jEventPublisher.__init__(self, driver)
        if logger is None:
            logger = logging.getLogger(__name__)
        self.__logger = logger

    @cached_property
    def logged_named(self) -> str:
        return super().logged_named

    async def _reserve_task(self, task: Task, project: str):
        task = task.dict(by_alias=True)
        task["inputs"] = json.dumps(task["inputs"])
        task.pop("status")
        task_id = task.pop("id")
        async with self._project_session(project) as sess:
            await sess.execute_write(_reserve_task_tx, task_id=task_id, task_props=task)

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

    @asynccontextmanager
    async def _project_session(
        self, project: str
    ) -> AsyncGenerator[neo4j.AsyncSession, None]:
        async with project_db_session(self._driver, project) as sess:
            yield sess

    @property
    def _logger(self) -> logging.Logger:
        return self.__logger

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._inherited_driver:
            await self._driver.__aexit__(exc_type, exc_val, exc_tb)


async def _reserve_task_tx(tx: neo4j.AsyncTransaction, task_id: str, task_props: Dict):
    query = f"""MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
RETURN task"""
    res = await tx.run(query, taskId=task_id)
    tasks = [rec async for rec in res]
    if tasks:  # The task has been registered following event publication
        task = Task.from_neo4j(tasks[0])
        if task.status > TaskStatus.QUEUED:
            # The task seems to be running elsewhere...
            raise TaskAlreadyReserved(task_id)
        task_props.pop("type")
        task_props.pop("createdAt")
    query = f"""MERGE (t:{TASK_NODE} {{{TASK_ID}: $taskId }})
SET t += $taskProps
WITH t CALL apoc.create.setLabels(t, $labels) YIELD node AS task
RETURN task"""
    labels = [TASK_NODE, TaskStatus.RUNNING.value]
    await tx.run(query, taskId=task_id, taskProps=task_props, labels=labels)


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
