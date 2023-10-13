from __future__ import annotations

import json
import logging
import multiprocessing
import threading
from abc import ABC
from datetime import datetime
from functools import cached_property
from multiprocessing import Queue
from pathlib import Path
from typing import Dict, List, Optional, Union

import neo4j
import pytest
import pytest_asyncio
from fastapi.encoders import jsonable_encoder

from neo4j_app.icij_worker import (
    EventPublisher,
    ICIJApp,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from neo4j_app.icij_worker.exceptions import (
    TaskAlreadyReserved,
    UnknownTask,
)
from neo4j_app.icij_worker.task_store import TaskStore
from neo4j_app.icij_worker.worker import ProcessWorkerMixin
from neo4j_app.typing_ import PercentProgress


@pytest_asyncio.fixture(scope="function")
async def populate_tasks(neo4j_app_driver: neo4j.AsyncDriver) -> List[Task]:
    query_0 = """CREATE (task:_Task:CREATED {
    id: 'task-0', 
    type: 'hello_world',
    createdAt: $now,
    inputs: '{"greeted": "0"}'
 }) 
RETURN task"""
    recs_0, _, _ = await neo4j_app_driver.execute_query(query_0, now=datetime.now())
    t_0 = Task.from_neo4j(recs_0[0])
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    type: 'hello_world',
    progress: 66.6,
    createdAt: $now,
    retries: 1,
    inputs: '{"greeted": "1"}'
 }) 
RETURN task"""
    recs_1, _, _ = await neo4j_app_driver.execute_query(query_1, now=datetime.now())
    t_1 = Task.from_neo4j(recs_1[0])
    return [t_0, t_1]


class DBMixin(ABC):
    _task_collection = "tasks"
    _error_collection = "errors"
    _result_collection = "results"

    def __init__(self, db_path: Path, lock: threading.Lock | multiprocessing.Lock):
        self._db_path = db_path
        self._lock = lock

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def lock(self) -> threading.Lock | multiprocessing.Lock:
        return self._lock

    def _write(self, data: Dict):
        self._db_path.write_text(json.dumps(jsonable_encoder(data)))

    def _read(self):
        return json.loads(self._db_path.read_text())

    @staticmethod
    def _task_key(task_id: str, project: str) -> str:
        return str((task_id, project))

    @classmethod
    def fresh_db(cls, db_path: Path):
        db = {
            cls._task_collection: dict(),
            cls._error_collection: {},
            cls._result_collection: {},
        }
        db_path.write_text(json.dumps(db))


class MockStore(TaskStore, DBMixin):
    async def get_task(self, *, project: str, task_id: str) -> Task:
        key = self._task_key(task_id=task_id, project=project)
        with self._lock:
            db = self._read()
        try:
            tasks = db[self._task_collection]
            return Task(**tasks[key])
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def get_task_errors(self, project: str, task_id: str) -> List[TaskError]:
        key = self._task_key(task_id=task_id, project=project)
        with self._lock:
            db = self._read()
        errors = db[self._error_collection]
        errors = errors.get(key, [])
        errors = [TaskError(**err) for err in errors]
        return errors

    async def get_task_result(self, project: str, task_id: str) -> TaskResult:
        key = self._task_key(task_id=task_id, project=project)
        with self._lock:
            db = self._read()
        results = db[self._result_collection]
        try:
            return TaskResult(**results[key])
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def get_tasks(
        self,
        project: str,
        task_type: Optional[str] = None,
        status: Optional[Union[List[TaskStatus], TaskStatus]] = None,
    ) -> List[Task]:
        with self._lock:
            db = self._read()
        tasks = db.values()
        if status:
            if isinstance(status, TaskStatus):
                status = [status]
            status = set(status)
            tasks = (t for t in tasks if t.status in status)
        return list(tasks)


class MockEventPublisher(DBMixin, EventPublisher):
    _excluded_from_event_update = {"error"}

    def __init__(self, db_path: Path, lock: threading.Lock | multiprocessing.Lock):
        super().__init__(db_path, lock)
        self.published_events = []

    async def publish_event(self, event: TaskEvent, project: str):
        self.published_events.append(event)
        # Let's simulate that we have an event handler which will reflect some event
        # into the DB, we could not do it. In this case tests should not expect that
        # events are reflected in the DB. They would only be registered inside
        # published_events (which could be enough).
        # Here we choose to reflect the change in the DB since its closer to what will
        # happen IRL and test integration further
        key = self._task_key(task_id=event.task_id, project=project)
        with self._lock:
            db = self._read()
            try:
                task = self._get_db_task(db, task_id=event.task_id, project=project)
                task = Task(**task)
            except UnknownTask:
                task = Task(**Task.mandatory_fields(event, keep_id=True))
            update = task.resolve_event(event)
            if update is not None:
                task = task.dict(exclude_unset=True, by_alias=True)
                update = {
                    k: v
                    for k, v in event.dict(by_alias=True, exclude_unset=True).items()
                    if v is not None
                }
                if "taskId" in update:
                    update["id"] = update.pop("taskId")
                if "taskType" in update:
                    update["type"] = update.pop("taskType")
                if "error" in update:
                    update.pop("error")
                task.update(update)
                db[self._task_collection][key] = task
                self._write(db)

    def _get_db_task(self, db: Dict, task_id: str, project: str) -> Dict:
        tasks = db[self._task_collection]
        try:
            return tasks[self._task_key(task_id=task_id, project=project)]
        except KeyError as e:
            raise UnknownTask(task_id) from e


class MockWorker(ProcessWorkerMixin, MockEventPublisher):
    from_config = ProcessWorkerMixin.from_config

    def __init__(
        self,
        app: ICIJApp,
        worker_id: str,
        queue: Queue,
        db_path: Path,
        lock: Union[threading.Lock, multiprocessing.Lock],
    ):
        super().__init__(app, worker_id, queue)
        MockEventPublisher.__init__(self, db_path, lock)
        self._worker_id = worker_id
        self.__logger = logging.getLogger(__name__)

    # TODO: not sure why this one is not inherited
    @cached_property
    def logged_named(self) -> str:
        return super().logged_named

    async def _reserve_task(self, task: Task, project: str):
        key = self._task_key(task_id=task.id, project=project)
        with self._lock:
            db = self._read()
            tasks = db[self._task_collection]
            existing = tasks.get(key, None)
            if existing and TaskStatus[existing["status"]] > TaskStatus.QUEUED:
                raise TaskAlreadyReserved(task.id)
            task = task.dict(exclude_unset=True, by_alias=True)
            tasks[key] = task
            self._write(db)

    async def _save_result(self, result: TaskResult, project: str):
        task_key = self._task_key(task_id=result.task_id, project=project)
        with self._lock:
            db = self._read()
            db[self._result_collection][task_key] = result
            self._write(db)

    async def _save_error(self, error: TaskError, task: Task, project: str):
        task_key = self._task_key(task_id=task.id, project=project)
        with self._lock:
            db = self._read()
            errors = db[self._error_collection].get(task_key)
            if errors is None:
                errors = []
            errors.append(error)
            db[self._error_collection][task_key] = errors
            self._write(db)

    @property
    def _logger(self) -> logging.Logger:
        return self.__logger

    def work(self):
        pass

    def _get_db_errors(self, task_id: str, project: str) -> List[TaskError]:
        key = self._task_key(task_id=task_id, project=project)
        with self._lock:
            db = self._read()
            errors = db[self._error_collection]
            try:
                return errors[key]
            except KeyError as e:
                raise UnknownTask(task_id) from e

    def _get_db_result(self, task_id: str, project: str) -> TaskResult:
        key = self._task_key(task_id=task_id, project=project)
        with self._lock:
            db = self._read()
            try:
                errors = db[self._result_collection]
                return errors[key]
            except KeyError as e:
                raise UnknownTask(task_id) from e


class Recoverable(ValueError):
    pass


@pytest.fixture(scope="function")
def test_failing_async_app() -> ICIJApp:
    app = ICIJApp(name="test-app")
    already_failed = False

    @app.task("recovering_task", recover_from=(Recoverable,))
    def _recovering_task() -> str:
        nonlocal already_failed
        if already_failed:
            return "i told you i could recover"
        already_failed = True
        raise Recoverable("i can recover from this")

    @app.task("fatal_error_task")
    async def _fatal_error_task(progress: Optional[PercentProgress] = None):
        if progress is not None:
            await progress(0.1)
        raise ValueError("this is fatal")

    return app
