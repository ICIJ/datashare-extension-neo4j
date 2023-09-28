import logging
from queue import Full
from typing import List

from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from neo4j_app.app.dependencies import (
    lifespan_event_publisher,
    lifespan_import_queue,
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
from neo4j_app.icij_worker.exceptions import UnknownTask

logger = logging.getLogger(__name__)


def tasks_router() -> APIRouter:
    router = APIRouter(tags=[TASKS_TAG])

    @router.post("/tasks", response_model=Task)
    async def _create_task(project: str, job: TaskJob) -> Response:
        import_queue = lifespan_import_queue()
        event_publisher = lifespan_event_publisher()
        task_id = job.task_id
        if task_id is None:
            task_id = job.generate_task_id()
        task = job.to_task(task_id=task_id)
        queued = (task, project)
        try:
            # TODO: improve this piece, since waiting for the job to be queued would
            #  involve blocking the HTTP server process we fail directly in case the
            #  queue is full
            import_queue.put(queued, block=False, timeout=None)
        except Full as e:
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

    @router.get("/tasks/{task_id}", response_model=Task)
    async def _get_task(task_id: str, project: str) -> Task:
        store = lifespan_task_store()
        try:
            task = await store.get_task(project=project, task_id=task_id)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return task

    @router.get("/tasks/{task_id}/result", response_model=object)
    async def _get_task_result(task_id: str, project: str) -> object:
        store = lifespan_task_store()
        try:
            result = await store.get_task_result(project=project, task_id=task_id)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return result.result

    @router.get("/tasks/{task_id}/errors", response_model=List[TaskError])
    async def _get_task_errors(task_id: str, project: str) -> List[TaskError]:
        store = lifespan_task_store()
        try:
            errors = await store.get_task_errors(project=project, task_id=task_id)
        except UnknownTask as e:
            raise HTTPException(status_code=404, detail=e.args[0]) from e
        return errors

    return router
