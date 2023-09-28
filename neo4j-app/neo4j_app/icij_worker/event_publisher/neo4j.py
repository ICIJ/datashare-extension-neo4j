from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, Optional

import neo4j

from neo4j_app.constants import (
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_ID,
    TASK_NODE,
)
from neo4j_app.core.neo4j.projects import project_db_session
from neo4j_app.icij_worker import Task, TaskEvent
from neo4j_app.icij_worker.exceptions import UnknownTask
from . import EventPublisher


class Neo4jEventPublisher(EventPublisher):
    def __init__(self, driver: neo4j.AsyncDriver):
        self._driver = driver

    async def publish_event(self, event: TaskEvent, project: str):
        async with self._project_session(project) as sess:
            await _publish_event(sess, event)

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver

    @asynccontextmanager
    async def _project_session(
        self, project: str
    ) -> AsyncGenerator[neo4j.AsyncSession, None]:
        async with project_db_session(self._driver, project) as sess:
            yield sess


async def _publish_event(sess: neo4j.AsyncSession, event: TaskEvent):
    event = {k: v for k, v in event.dict(by_alias=True).items() if v is not None}
    if "status" in event:
        event["status"] = event["status"].value
    error = event.pop("error", None)
    await sess.execute_write(_publish_event_tx, event, error)


async def _publish_event_tx(
    tx: neo4j.AsyncTransaction, event: Dict, error: Optional[Dict]
):
    task_id = event["taskId"]
    create_task = f"""MERGE (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
ON CREATE SET task += $createProps
RETURN task"""
    event_as_event = TaskEvent(**event)
    create_props = Task.mandatory_fields(event_as_event, keep_id=False)
    if "status" in create_props:
        create_props["status"] = create_props["status"].value
    res = await tx.run(create_task, taskId=task_id, createProps=create_props)
    tasks = [Task.from_neo4j(rec) async for rec in res]
    task = tasks[0]
    resolved = task.resolve_event(event_as_event)
    resolved = (
        resolved.dict(exclude_unset=True, by_alias=True)
        if resolved is not None
        else resolved
    )
    if resolved:
        update_task = f"""MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
SET task += $updateProps
RETURN count(*) as numTasks"""
        if "status" in resolved:
            resolved["status"] = resolved["status"].value
        resolved.pop("taskId")
        task_type = resolved.pop("taskType", None)
        if task_type is not None:
            resolved["type"] = task_type
        res = await tx.run(update_task, taskId=task_id, updateProps=resolved)
        num_tasks = await res.single()
        num_tasks = num_tasks["numTasks"]
        if not num_tasks:
            raise UnknownTask(task_id)
    if error is not None:
        create_error = f"""MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
MERGE (error:{TASK_ERROR_NODE} {{id: $errorId}})
ON CREATE 
    SET error = $errorProps
MERGE (error)-[:{TASK_ERROR_OCCURRED_TYPE}]->(task)"""
        error_id = error.pop("id")
        await tx.run(create_error, taskId=task_id, errorId=error_id, errorProps=error)
