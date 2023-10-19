import logging
from typing import List

from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from neo4j_app.app.dependencies import (
    lifespan_event_publisher,
    lifespan_task_manager,
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
        task_task_manager = lifespan_task_manager()
        event_publisher = lifespan_event_publisher()
        task_id = job.task_id
        if task_id is None:
            task_id = job.generate_task_id()
        task = job.to_task(task_id=task_id)
        try:
            await task_task_manager.enqueue(task, project)
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
        task_task_manager = lifespan_task_manager()
        try:
            cancelled = await task_task_manager.cancel(task_id=task_id, project=project)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return cancelled

    @router.get("/tasks/{task_id}", response_model=Task)
    async def _get_task(task_id: str, project: str) -> Task:
        task_manager = lifespan_task_manager()
        try:
            task = await task_manager.get_task(task_id=task_id, project=project)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return task

    @router.get("/tasks/{task_id}/result", response_model=object)
    async def _get_task_result(task_id: str, project: str) -> object:
        task_manager = lifespan_task_manager()
        try:
            result = await task_manager.get_task_result(
                task_id=task_id, project=project
            )
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return result.result

    @router.get("/tasks/{task_id}/errors", response_model=List[TaskError])
    async def _get_task_errors(task_id: str, project: str) -> List[TaskError]:
        task_manager = lifespan_task_manager()
        try:
            errors = await task_manager.get_task_errors(
                task_id=task_id, project=project
            )
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return errors

    return router
