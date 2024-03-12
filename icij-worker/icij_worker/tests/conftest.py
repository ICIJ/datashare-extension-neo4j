# pylint: disable=redefined-outer-name
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import ClassVar, Dict, List, Optional, Tuple, Union

import neo4j
import pytest
import pytest_asyncio
from icij_common.neo4j.migrate import (
    Migration,
    init_project,
)
from icij_common.neo4j.projects import add_project_support_migration_tx
from icij_common.neo4j.test_utils import (  # pylint: disable=unused-import
    neo4j_test_driver,
)
from icij_common.pydantic_utils import IgnoreExtraModel, safe_copy
from icij_common.test_utils import TEST_PROJECT
from pydantic import Field

import icij_worker
from icij_worker import (
    AsyncApp,
    EventPublisher,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.exceptions import TaskAlreadyExists, TaskQueueIsFull, UnknownTask
from icij_worker.task_manager import TaskManager
from icij_worker.task_manager.neo4j import add_support_for_async_task_tx
from icij_worker.typing_ import PercentProgress

# noinspection PyUnresolvedReferences
from icij_worker.utils.tests import (
    DBMixin,
    test_async_app,
)

logger = logging.getLogger(__name__)


async def migration_v_0_1_0_tx(tx: neo4j.AsyncTransaction):
    await add_project_support_migration_tx(tx)
    await add_support_for_async_task_tx(tx)


TEST_MIGRATIONS = [
    Migration(
        version="0.1.0",
        label="create migration and project and constraints as well as task"
        " related stuff",
        migration_fn=migration_v_0_1_0_tx,
    )
]


@pytest_asyncio.fixture(scope="function")
async def populate_tasks(neo4j_async_app_driver: neo4j.AsyncDriver) -> List[Task]:
    query_0 = """CREATE (task:_Task:QUEUED {
    id: 'task-0', 
    type: 'hello_world',
    createdAt: $now,
    inputs: '{"greeted": "0"}'
 }) 
RETURN task"""
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_0, now=datetime.now()
    )
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
    recs_1, _, _ = await neo4j_async_app_driver.execute_query(
        query_1, now=datetime.now()
    )
    t_1 = Task.from_neo4j(recs_1[0])
    return [t_0, t_1]


class MockManager(TaskManager, DBMixin):
    def __init__(self, db_path: Path, max_queue_size: int):
        super().__init__(db_path)
        self._max_queue_size = max_queue_size

    async def _enqueue(self, task: Task, project: str) -> Task:
        key = self._task_key(task_id=task.id, project=project)
        db = self._read()
        tasks = db[self._task_collection]
        n_queued = sum(
            1 for t in tasks.values() if t["status"] == TaskStatus.QUEUED.value
        )
        if n_queued > self._max_queue_size:
            raise TaskQueueIsFull(self._max_queue_size)
        if key in tasks:
            raise TaskAlreadyExists(task.id)
        update = {"status": TaskStatus.QUEUED}
        task = safe_copy(task, update=update)
        tasks[key] = task.dict()
        self._write(db)
        return task

    async def _cancel(self, *, task_id: str, project: str) -> Task:
        key = self._task_key(task_id=task_id, project=project)
        task_id = await self.get_task(task_id=task_id, project=project)
        update = {"status": TaskStatus.CANCELLED}
        task_id = safe_copy(task_id, update=update)
        db = self._read()
        db[self._task_collection][key] = task_id.dict()
        self._write(db)
        return task_id

    async def get_task(self, *, task_id: str, project: str) -> Task:
        key = self._task_key(task_id=task_id, project=project)
        db = self._read()
        try:
            tasks = db[self._task_collection]
            return Task(**tasks[key])
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def get_task_errors(self, task_id: str, project: str) -> List[TaskError]:
        key = self._task_key(task_id=task_id, project=project)
        db = self._read()
        errors = db[self._error_collection]
        errors = errors.get(key, [])
        errors = [TaskError(**err) for err in errors]
        return errors

    async def get_task_result(self, task_id: str, project: str) -> TaskResult:
        key = self._task_key(task_id=task_id, project=project)
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

    def __init__(self, db_path: Path):
        super().__init__(db_path)
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
            # The nack is responsible for bumping the retries
            if "retries" in update:
                update.pop("retries")
            task.update(update)
            db[self._task_collection][key] = task
            self._write(db)

    def _get_db_task(self, db: Dict, task_id: str, project: str) -> Dict:
        tasks = db[self._task_collection]
        try:
            return tasks[self._task_key(task_id=task_id, project=project)]
        except KeyError as e:
            raise UnknownTask(task_id) from e


@WorkerConfig.register()
class MockWorkerConfig(WorkerConfig, IgnoreExtraModel):
    type: ClassVar[str] = Field(const=True, default=WorkerType.mock.value)
    log_level: str = "DEBUG"
    loggers: List[str] = [icij_worker.__name__]
    db_path: Path


@Worker.register(WorkerType.mock)
class MockWorker(Worker, MockEventPublisher):
    def __init__(
        self,
        app: AsyncApp,
        worker_id: str,
        db_path: Path,
        **kwargs,
    ):
        super().__init__(app, worker_id, **kwargs)
        MockEventPublisher.__init__(self, db_path)
        self._worker_id = worker_id
        self._logger_ = logging.getLogger(__name__)

    @classmethod
    def _from_config(cls, config: MockWorkerConfig, **extras) -> MockWorker:
        worker = cls(db_path=config.db_path, **extras)
        return worker

    def _to_config(self) -> MockWorkerConfig:
        return MockWorkerConfig(db_path=self._db_path)

    async def _save_result(self, result: TaskResult, project: str):
        task_key = self._task_key(task_id=result.task_id, project=project)
        db = self._read()
        db[self._result_collection][task_key] = result
        self._write(db)

    async def _save_error(self, error: TaskError, task: Task, project: str):
        task_key = self._task_key(task_id=task.id, project=project)
        db = self._read()
        errors = db[self._error_collection].get(task_key)
        if errors is None:
            errors = []
        errors.append(error)
        db[self._error_collection][task_key] = errors
        self._write(db)

    def _get_db_errors(self, task_id: str, project: str) -> List[TaskError]:
        key = self._task_key(task_id=task_id, project=project)
        db = self._read()
        errors = db[self._error_collection]
        try:
            return errors[key]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    def _get_db_result(self, task_id: str, project: str) -> TaskResult:
        key = self._task_key(task_id=task_id, project=project)
        db = self._read()
        try:
            errors = db[self._result_collection]
            return errors[key]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def _acknowledge(self, task: Task, project: str, completed_at: datetime):
        key = self._task_key(task.id, project)
        db = self._read()
        tasks = db[self._task_collection]
        try:
            saved_task = tasks[key]
        except KeyError as e:
            raise UnknownTask(task.id) from e
        saved_task = Task(**saved_task)
        update = {
            "completed_at": completed_at,
            "status": TaskStatus.DONE,
            "progress": 100.0,
        }
        tasks[key] = safe_copy(saved_task, update=update)
        self._write(db)

    async def _negatively_acknowledge(
        self, task: Task, project: str, *, requeue: bool
    ) -> Task:
        key = self._task_key(task.id, project)
        db = self._read()
        tasks = db[self._task_collection]
        try:
            task = tasks[key]
        except KeyError as e:
            raise UnknownTask(task_id=task.id) from e
        task = Task(**task)
        if requeue:
            update = {
                "status": TaskStatus.QUEUED,
                "progress": 0.0,
                "retries": task.retries or 0 + 1,
            }
        else:
            update = {"status": TaskStatus.ERROR}
        task = safe_copy(task, update=update)
        tasks[key] = task
        self._write(db)
        return task

    async def _refresh_cancelled(self, project: str):
        db = self._read()
        tasks = db[self._task_collection]
        tasks = [Task(**t) for t in tasks.values()]
        cancelled = [t.id for t in tasks if t.status is TaskStatus.CANCELLED]
        self._cancelled_[project] = set(cancelled)

    async def _consume(self) -> Tuple[Task, str]:
        while "waiting for some task to be available for some project":
            db = self._read()
            tasks = db[self._task_collection]
            tasks = [(k, Task(**t)) for k, t in tasks.items()]
            queued = [(k, t) for k, t in tasks if t.status is TaskStatus.QUEUED]
            if queued:
                k, t = min(queued, key=lambda x: x[1].created_at)
                project = eval(k)[1]  # pylint: disable=eval-used
                return t, project
            await asyncio.sleep(self.config.task_queue_poll_interval_s)


class Recoverable(ValueError):
    pass


@pytest.fixture(scope="function")
def test_failing_async_app() -> AsyncApp:
    # TODO: add log deps here if it helps to debug
    app = AsyncApp(name="test-app", dependencies=[])
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


@pytest.fixture()
async def neo4j_async_app_driver(
    neo4j_test_driver: neo4j.AsyncDriver,
) -> neo4j.AsyncDriver:
    await init_project(
        neo4j_test_driver,
        name=TEST_PROJECT,
        registry=TEST_MIGRATIONS,
        timeout_s=0.001,
        throttle_s=0.001,
    )
    return neo4j_test_driver
