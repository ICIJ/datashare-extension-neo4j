import logging
from typing import List

from fastapi import APIRouter, HTTPException
from icij_common.logging_utils import log_elapsed_time_cm
from icij_worker import (
    Task,
    TaskError,
    TaskEvent,
    TaskStatus,
)
from icij_worker.exceptions import (
    TaskAlreadyExists,
    TaskQueueIsFull,
    UnknownTask,
)
from starlette.responses import Response

from neo4j_app.app.dependencies import (
    lifespan_event_publisher,
    lifespan_task_manager,
)
from neo4j_app.app.doc import TASKS_TAG
from neo4j_app.core.objects import TaskJob, TaskSearch

logger = logging.getLogger(__name__)


def tasks_router() -> APIRouter:
    router = APIRouter(tags=[TASKS_TAG])

    @router.post("/tasks", response_model=Task)
    async def _create_task(project: str, job: TaskJob) -> Response:
        task_manager = lifespan_task_manager()
        event_publisher = lifespan_event_publisher()
        task_id = job.task_id
        if task_id is None:
            task_id = job.generate_task_id()
        task = job.to_task(task_id=task_id)
        try:
            await task_manager.enqueue(task, project)
        except TaskAlreadyExists:
            return Response(task.id, status_code=200)
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
        task_manager = lifespan_task_manager()
        try:
            cancelled = await task_manager.cancel(task_id=task_id, project=project)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return cancelled

    @router.get("/tasks/{task_id}", response_model=Task)
    async def _get_task(task_id: str, project: str) -> Task:
        task_manager = lifespan_task_manager()
        try:
            with log_elapsed_time_cm(
                logger, logging.INFO, "retrieved task in {elapsed_time} !"
            ):
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

    @router.post("/tasks/search", response_model=List[Task])
    async def _search_tasks(project: str, search: TaskSearch) -> List[Task]:
        task_manager = lifespan_task_manager()
        with log_elapsed_time_cm(logger, TRACE, "Searched tasks in {elapsed_time} !"):
            tasks = await task_manager.get_tasks(
                project=project, task_type=search.type, status=search.status
            )
        return tasks

    return router
