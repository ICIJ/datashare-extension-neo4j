from __future__ import annotations

import asyncio
import functools
import inspect
import sys
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from copy import deepcopy
from datetime import datetime
from inspect import isawaitable, signature
from typing import (
    Any,
    AsyncContextManager,
    Callable,
    Dict,
    Optional,
    Tuple,
    Type,
    cast,
    final,
)

from neo4j_app.core import AppConfig
from neo4j_app.core.utils.logging import LogWithNameMixin
from neo4j_app.icij_worker.app import ICIJApp, RegisteredTask
from neo4j_app.icij_worker.event_publisher import EventPublisher
from neo4j_app.icij_worker.exceptions import MaxRetriesExceeded, UnregisteredTask
from neo4j_app.icij_worker.task import (
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)

PROGRESS_HANDLER_ARG = "progress"


class Worker(EventPublisher, LogWithNameMixin, AbstractAsyncContextManager, ABC):
    def __init__(self, app: ICIJApp, worker_id: str):
        self._app = app
        self._id = worker_id
        self._graceful_shutdown = True
        self._config = app.config

    @classmethod
    def from_config(cls, config: AppConfig, worker_id: str, **kwargs) -> Worker:
        worker_cls = config.to_worker_cls()
        return worker_cls(app=config.to_async_app(), worker_id=worker_id, **kwargs)

    @classmethod
    @final
    def work_forever_from_config(cls, config: AppConfig, worker_id: str, **kwargs):
        worker = cls.from_config(config, worker_id, **kwargs)

        asyncio.run(worker.work_forever())

    @final
    async def work_forever(self):
        self._app.config.setup_loggers()
        async with self:
            self.info("started working...")
            exit_status = 0
            try:
                while True:
                    await self.work_once()
            except KeyboardInterrupt:
                self.info("shutting down...")
            except Exception as e:
                self.error("error occurred while consuming: %s", e)
                self.info("will try to shutdown gracefully...")
                exit_status = 1
                raise e
            finally:
                if self.graceful_shutdown:
                    self.info("shutting down gracefully...")
                    await self.__aexit__(*sys.exc_info())
                else:
                    self.info("shutting down the hard way...")
                    sys.exit(exit_status)
        sys.exit(exit_status)

    @final
    @functools.cached_property
    def logged_name(self) -> str:
        return self._id

    @property
    def graceful_shutdown(self) -> bool:
        return self._graceful_shutdown

    @final
    async def work_once(self):
        task, project = await self.receive()
        progress = functools.partial(self._publish_progress, task=task, project=project)
        await task_wrapper(
            task, project, self, config=self._app.config, progress=progress
        )

    @abstractmethod
    async def receive(self) -> Tuple[Task, str]:
        pass

    @final
    async def reserve_task(self, task: Task, project: str):
        await self._reserve_task(task, project)
        event = TaskEvent(task_id=task.id, progress=0, status=TaskStatus.RUNNING)
        await self.publish_event(event, project)

    @abstractmethod
    async def _reserve_task(self, task: Task, project: str):
        pass

    @abstractmethod
    async def _save_result(self, result: TaskResult, project: str):
        """Save the result in a safe place"""

    @abstractmethod
    async def _save_error(self, error: TaskError, task: Task, project: str):
        """Save the error in a safe place"""

    @final
    async def save_result(
        self, result: TaskResult, project: str, completed_at: datetime
    ):
        self.info("saving task id=%s", result.task_id)
        await self._save_result(result, project)
        # Once the result has been saved, we notify the event consumers, they are
        # responsible for reflecting the fact that task has completed wherever relevant.
        # The source of truth will be result storage
        self.info("marking (id=%s) as %s", result.task_id, TaskStatus.DONE)
        event = TaskEvent(
            task_id=result.task_id,
            status=TaskStatus.DONE,
            progress=100,
            completed_at=completed_at,
        )
        # Tell the listeners that the task succeeded
        await self.publish_event(event, project)

    @final
    async def save_error(
        self, error: TaskError, task: Task, project: str, retries: Optional[int] = None
    ):
        self.error("(id=%s): %s\n%s", task.id, error.title, error.detail)
        # Save the error in the appropriate location
        self.debug("(id=%s) saving error", task.id, error)
        await self._save_error(error, task, project)
        # Once the error has been saved, we notify the event consumers, they are
        # responsible for reflecting the fact that the error has occurred wherever
        # relevant. The source of truth will be error storage
        await self.publish_error_event(
            error=error, task_id=task.id, project=project, retries=retries
        )

    @final
    async def publish_error_event(
        self,
        *,
        error: TaskError,
        task_id: str,
        project: str,
        retries: Optional[int] = None,
    ):
        # Tell the listeners that the task failed
        self.debug("(id=%s) publish error event", task_id)
        event = TaskEvent.from_error(error, task_id, retries)
        await self.publish_event(event, project)

    @final
    async def _publish_progress(self, progress: float, task: Task, project: str):
        event = TaskEvent(progress=progress, task_id=task.id)
        await self.publish_event(event, project)

    @final
    def parse_task(
        self, task: Task, project: str
    ) -> Tuple[Callable, Tuple[Type[Exception], ...]]:
        registered = _retrieve_registered_task(task, self._app)
        recoverable = registered.recover_from
        task_fn = registered.task
        supports_progress = any(
            param.name == PROGRESS_HANDLER_ARG
            for param in signature(task_fn).parameters.values()
        )
        if supports_progress:
            publish_progress = functools.partial(
                self._publish_progress, project=project, task=task
            )
            task_fn = functools.partial(task_fn, progress=publish_progress)
        return task_fn, recoverable

    @final
    def check_retries(self, retries: int, task: Task):
        max_retries = self._app.registry[task.type].max_retries
        if max_retries is None:
            return
        self.info("%s(id=%s): try %s/%s", task.type, task.id, retries, max_retries)
        if retries is not None and retries > max_retries:
            raise MaxRetriesExceeded(
                f"{task.type}(id={task.id}): max retries exceeded > {max_retries}"
            )

    @final
    @property
    @asynccontextmanager
    async def _deps_cm(self):
        if self._config is not None:
            from neo4j_app.app.dependencies import run_deps

            async with run_deps(self._config, self._config.to_async_deps()):
                yield
        else:
            yield

    async def __aenter__(self):
        await self._deps_cm.__aenter__()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._deps_cm.__aexit__(exc_type, exc_value, traceback)


def _retrieve_registered_task(
    task: Task,
    app: ICIJApp,
) -> RegisteredTask:
    registered = app.registry.get(task.type)
    if registered is None:
        available_tasks = list(app.registry)
        raise UnregisteredTask(task.type, available_tasks)
    return registered


async def task_wrapper(task: Task, project: str, worker: Worker, **kwargs):
    retries = 0
    try:
        await worker.reserve_task(task, project)
        task_fn, recoverable_errors = worker.parse_task(task, project)
    except Exception as e:  # pylint: disable=broad-except
        error = TaskError.from_exception(e)
        await worker.save_error(error=error, task=task, project=project)
        return

    while True:
        try:
            worker.check_retries(retries, task)
            if retries:
                # In the case of the retry, let's reset the progress
                event = TaskEvent(task_id=task.id, progress=0.0)
                await worker.publish_event(event, project)
            try:
                all_inputs = add_missing_args(task_fn, task.inputs, **kwargs)
                task_res = task_fn(**all_inputs)
                if isawaitable(task_res):
                    task_res = await task_res
                completed_at = datetime.now()
            except recoverable_errors as e:
                retries += 1
                error = TaskError.from_exception(e)
                await worker.publish_error_event(
                    error=error, task_id=task.id, project=project, retries=retries
                )
                continue
            result = TaskResult(task_id=task.id, result=task_res)
            await worker.save_result(result, project, completed_at=completed_at)
            worker.info("task %s successful !", task.id)
        except Exception as e:  # pylint: disable=broad-except
            error = TaskError.from_exception(e)
            await worker.save_error(error=error, task=task, project=project)
            if isinstance(e, MaxRetriesExceeded):
                worker.error("(id=%s) exceeded max retries, exiting !", task.id)
            else:
                worker.error("(id=%s) fatal error, exiting !", task.id)
        return


def add_missing_args(fn: Callable, inputs: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    # We make the choice not to raise in case of missing argument here, the error will
    # be correctly raise when the function is called
    from_kwargs = dict()
    sig = inspect.signature(fn)
    for param_name in sig.parameters:
        if param_name in inputs:
            continue
        kwargs_value = kwargs.get(param_name)
        if kwargs_value is not None:
            from_kwargs[param_name] = kwargs_value
    if from_kwargs:
        inputs = deepcopy(inputs)
        inputs.update(from_kwargs)
    return inputs
