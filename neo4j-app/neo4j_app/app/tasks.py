import logging
from typing import List

from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from neo4j_app.app.dependencies import (
    lifespan_event_publisher,
    lifespan_task_store,
)
from neo4j_app.app.doc import TASKS_TAG
from neo4j_app.core.objects import TaskJob
from neo4j_app.icij_worker import (
    Task,
    TaskError,
    TaskEvent,
    TaskStatus,
)
from neo4j_app.icij_worker.exceptions import TaskQueueIsFull, UnknownTask

logger = logging.getLogger(__name__)


def tasks_router() -> APIRouter:
    router = APIRouter(tags=[TASKS_TAG])

    @router.post("/tasks", response_model=Task)
    async def _create_task(project: str, job: TaskJob) -> Response:
        task_store = lifespan_task_store()
        event_publisher = lifespan_event_publisher()
        task_id = job.task_id
        if task_id is None:
            task_id = job.generate_task_id()
        task = job.to_task(task_id=task_id)
        try:
            await task_store.enqueue(task, project)
        except TaskQueueIsFull as e:
            raise HTTPException(429, detail="Too Many Requests") from e
        logger.debug("Publishing task queuing event...")
        event = TaskEvent(
            task_id=task.id,
            task_type=task.type,
            status=TaskStatus.QUEUED,
            created_at=task.created_at,
        )
        await event_publisher.publish_event(event, project)
        return Response(task.id, status_code=201)

    @router.post("/tasks/{task_id}/cancel", response_model=Task)
    async def _cancel_task(project: str, task_id: str) -> Task:
        task_store = lifespan_task_store()
        try:
            cancelled = await task_store.cancel(task_id=task_id, project=project)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return cancelled

    @router.get("/tasks/{task_id}", response_model=Task)
    async def _get_task(task_id: str, project: str) -> Task:
        store = lifespan_task_store()
        try:
            task = await store.get_task(task_id=task_id, project=project)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return task

    @router.get("/tasks/{task_id}/result", response_model=object)
    async def _get_task_result(task_id: str, project: str) -> object:
        store = lifespan_task_store()
        try:
            result = await store.get_task_result(task_id=task_id, project=project)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return result.result

    @router.get("/tasks/{task_id}/errors", response_model=List[TaskError])
    async def _get_task_errors(task_id: str, project: str) -> List[TaskError]:
        store = lifespan_task_store()
        try:
            errors = await store.get_task_errors(task_id=task_id, project=project)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return errors

    return router
