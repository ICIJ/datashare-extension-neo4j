from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import traceback
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from copy import deepcopy
from datetime import datetime
from inspect import isawaitable, signature
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    final,
)

from neo4j_app.core import AppConfig
from neo4j_app.core.utils.logging import LogWithNameMixin
from neo4j_app.core.utils.progress import CheckCancelledProgress
from neo4j_app.icij_worker.app import ICIJApp, RegisteredTask
from neo4j_app.icij_worker.event_publisher import EventPublisher
from neo4j_app.icij_worker.exceptions import (
    MaxRetriesExceeded,
    RecoverableError,
    TaskAlreadyReserved,
    TaskCancelled,
    UnregisteredTask,
    WorkerCancelled,
)
from neo4j_app.icij_worker.task import (
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from neo4j_app.typing_ import PercentProgress

logger = logging.getLogger(__name__)

PROGRESS_HANDLER_ARG = "progress"


class Worker(EventPublisher, LogWithNameMixin, AbstractAsyncContextManager, ABC):
    def __init__(self, app: ICIJApp, worker_id: str):
        if app.config is None:
            raise ValueError("worker requires a configured app, app config is missing")
        self._app = app
        self._id = worker_id
        self._graceful_shutdown = True
        self._config = app.config
        self._cancelled_ = defaultdict(set)
        self.__deps_cm = None
        self._current = None

    @property
    def _cancelled(self) -> List[str]:
        return list(self._cancelled_)

    @functools.cached_property
    def id(self) -> str:
        return self._id

    @classmethod
    def from_config(cls, config: AppConfig, worker_id: str, **kwargs) -> Worker:
        worker_cls = config.to_worker_cls()
        return worker_cls(app=config.to_async_app(), worker_id=worker_id, **kwargs)

    @classmethod
    @final
    def work_forever_from_config(cls, config: AppConfig, worker_id: str, **kwargs):
        asyncio.run(cls.work_forever_from_config_async(config, worker_id, **kwargs))

    @classmethod
    @final
    async def work_forever_from_config_async(
        cls, config: AppConfig, worker_id: str, **kwargs
    ):
        worker = cls.from_config(config, worker_id, **kwargs)
        await worker.work_forever()

    @final
    async def work_forever(self):
        async with self:
            self.info("started working...")
            try:
                while True:
                    await self.work_once()
            except WorkerCancelled:
                self.info("worker cancelled, shutting down...")
            except Exception as e:
                self.error("error occurred while consuming: %s", _format_error(e))
                self.info("will try to shutdown gracefully...")
                raise e
            finally:
                await self.shutdown()

    @final
    @functools.cached_property
    def config(self) -> AppConfig:
        return self._config

    @final
    @functools.cached_property
    def logged_name(self) -> str:
        return self.id

    @property
    def graceful_shutdown(self) -> bool:
        return self._graceful_shutdown

    @final
    async def work_once(self):
        await task_wrapper(self)

    @final
    async def consume(self) -> Tuple[Task, str]:
        return await self._consume()

    @final
    @asynccontextmanager
    async def acknowledgment_cm(self, task: Task, project: str):
        async with self._persist_error(task, project):
            self._current = task, project
            self.debug('Task(id="%s") locked', task.id)
            try:
                event = TaskEvent(
                    task_id=task.id, progress=0, status=TaskStatus.RUNNING
                )
                await self.publish_event(event, project)
                yield
                await self.acknowledge(task, project)
            except RecoverableError:
                self.error('Task(id="%s") encountered error', task.id)
                await self.negatively_acknowledge(task, project, requeue=True)
            except Exception as fatal_error:
                await self.negatively_acknowledge(task, project, requeue=False)
                raise fatal_error
            self._current = None
            self.info('Task(id="%s") successful !', task.id)

    @final
    async def acknowledge(self, task: Task, project: str):
        completed_at = datetime.now()
        self.info('Task(id="%s") acknowledging...', task.id)
        await self._acknowledge(task, project, completed_at)
        self.info('Task(id="%s") acknowledged', task.id)
        self.debug('Task(id="%s") publishing acknowledgement event', task.id)
        event = TaskEvent(
            task_id=task.id,
            status=TaskStatus.DONE,
            progress=100,
            completed_at=completed_at,
        )
        # Tell the listeners that the task succeeded
        await self.publish_event(event, project)

    @abstractmethod
    async def _acknowledge(
        self, task: Task, project: str, completed_at: datetime
    ) -> Task:
        pass

    @final
    async def negatively_acknowledge(
        self, task: Task, project: str, *, requeue: bool
    ) -> Task:
        self.info(
            "negatively acknowledging Task(id=%s)w with (requeue=%s)...",
            task.id,
            requeue,
        )
        nacked = await self._negatively_acknowledge(task, project, requeue=requeue)
        self.info("Task(id=%s) negatively acknowledged (requeue=%s)!", task.id, requeue)
        return nacked

    @abstractmethod
    async def _negatively_acknowledge(
        self, task: Task, project: str, *, requeue: bool
    ) -> Task:
        pass

    @abstractmethod
    async def _refresh_cancelled(self, project: str):
        pass

    @abstractmethod
    async def _consume(self) -> Tuple[Task, str]:
        pass

    @abstractmethod
    async def _save_result(self, result: TaskResult, project: str):
        """Save the result in a safe place"""

    @abstractmethod
    async def _save_error(self, error: TaskError, task: Task, project: str):
        """Save the error in a safe place"""

    @final
    async def save_result(self, result: TaskResult, project: str):
        self.info('Task(id="%s") saving result...', result.task_id)
        await self._save_result(result, project)
        self.info('Task(id="%s") result saved !', result.task_id)

    @final
    async def save_error(self, error: TaskError, task: Task, project: str):
        self.error('Task(id="%s"): %s\n%s', task.id, error.title, error.detail)
        # Save the error in the appropriate location
        self.debug('Task(id="%s") saving error', task.id, error)
        await self._save_error(error, task, project)
        # Once the error has been saved, we notify the event consumers, they are
        # responsible for reflecting the fact that the error has occurred wherever
        # relevant. The source of truth will be error storage
        await self.publish_error_event(error=error, task_id=task.id, project=project)

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
        self.debug('Task(id="%s") publish error event', task_id)
        event = TaskEvent.from_error(error, task_id, retries)
        await self.publish_event(event, project)

    @final
    async def _publish_progress(self, progress: float, task: Task, project: str):
        event = TaskEvent(progress=progress, task_id=task.id)
        await self.publish_event(event, project)

    @final
    @asynccontextmanager
    async def _persist_error(self, task: Task, project: str):
        try:
            yield
        except Exception as e:  # pylint: disable=broad-except
            if isinstance(e, MaxRetriesExceeded):
                self.error('Task(id="%s") exceeded max retries, exiting !', task.id)
            else:
                self.error('Task(id="%s") fatal error, exiting !', task.id)
            error = TaskError.from_exception(e)
            await self.save_error(error=error, task=task, project=project)

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
            publish_progress = self._make_progress(task, project)
            task_fn = functools.partial(task_fn, progress=publish_progress)
        return task_fn, recoverable

    @final
    @functools.cached_property
    def _cancelled_task_refresh_interval_s(self) -> int:
        return self._app.config.neo4j_app_cancelled_task_refresh_interval_s

    @final
    async def check_cancelled(
        self, *, task_id: str, project: str, refresh: bool = False
    ):
        if refresh:
            await self._refresh_cancelled(project)
        if task_id in self._cancelled_[project]:
            raise TaskCancelled(task_id)

    @final
    def check_retries(self, retries: int, task: Task):
        max_retries = self._app.registry[task.type].max_retries
        if max_retries is None:
            return
        self.info(
            '%sTask(id="%s"): try %s/%s', task.type, task.id, retries, max_retries
        )
        if retries is not None and retries > max_retries:
            raise MaxRetriesExceeded(
                f"{task.type}(id={task.id}): max retries exceeded > {max_retries}"
            )

    @final
    def _make_progress(self, task: Task, project: str) -> PercentProgress:
        progress = functools.partial(self._publish_progress, task=task, project=project)
        refresh = functools.partial(self._refresh_cancelled, project=project)
        check = functools.partial(self.check_cancelled, project=project)
        progress = CheckCancelledProgress(
            task_id=task.id,
            progress=progress,
            check_cancelled=check,
            refresh_cancelled=refresh,
            refresh_interval_s=self._cancelled_task_refresh_interval_s,
        )
        return progress

    @final
    @asynccontextmanager
    async def _deps_cm(self):
        if self._config is not None:
            from neo4j_app.app.dependencies import run_deps

            async with run_deps(self._config, self._config.to_async_deps()):
                yield
        else:
            yield

    @final
    async def __aenter__(self):
        self.__deps_cm = self._deps_cm()
        await self.__deps_cm.__aenter__()
        await self._aenter__()

    async def _aenter__(self):
        pass

    @final
    async def __aexit__(self, exc_type, exc_value, tb):
        await self._aexit__(exc_type, exc_value, tb)
        await self._shutdown_gracefully()

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @final
    async def _shutdown_gracefully(self):
        self.info("cancelling running task...")
        await self._negatively_acknowledge_running_task(requeue=True)
        self.info("closing dependencies...")
        await self.__deps_cm.__aexit__(None, None, None)
        self.info("work shut down complete !")

    async def _negatively_acknowledge_running_task(self, requeue: bool):
        if self._current:
            task, project = self._current
            await self.negatively_acknowledge(task, project, requeue=requeue)

    @final
    async def shutdown(self):
        if self.graceful_shutdown:
            self.info("shutting down gracefully")
            await self._shutdown_gracefully()
            self.info("graceful shut down complete")
        else:
            self.info("shutting down the hard way...")


def _retrieve_registered_task(
    task: Task,
    app: ICIJApp,
) -> RegisteredTask:
    registered = app.registry.get(task.type)
    if registered is None:
        available_tasks = list(app.registry)
        raise UnregisteredTask(task.type, available_tasks)
    return registered


async def task_wrapper(worker: Worker):
    # Receive task
    try:
        task, project = await worker.consume()
    except TaskAlreadyReserved:
        # This part is won't happen with AMQP since it will take care to correctly
        # forward one task to one worker
        worker.info("tried to consume an already reserved task, exiting...")
        return
    except Exception as e:  # pylint: disable=broad-exception-caught
        worker.error("error while receiving task: %s", _format_error(e))
        raise e

    # Lock it and skips if already reserved
    async with worker.acknowledgment_cm(task, project):
        # Skip it if already cancelled
        try:
            await worker.check_cancelled(task_id=task.id, project=project, refresh=True)
        except TaskCancelled:
            worker.info('Task(id="%s") already cancelled skipping it !', task.id)
            return

        # Parse task to retrieve recoverable errors and max retries
        task_fn, recoverable_errors = worker.parse_task(task, project)
        task_inputs = add_missing_args(
            task_fn, task.inputs, config=worker.config, project=project
        )
        # Retry task until success, fatal error or max retry exceeded
        await _retry_task(
            worker, task, task_fn, task_inputs, project, recoverable_errors
        )


async def _retry_task(
    worker: Worker,
    task: Task,
    task_fn: Callable,
    task_inputs: Dict,
    project: str,
    recoverable_errors: Tuple[Type[Exception]],
):
    retries = task.retries or 0
    if retries:
        # In the case of the retry, let's reset the progress
        event = TaskEvent(task_id=task.id, progress=0.0)
        await worker.publish_event(event, project)
    try:
        task_res = task_fn(**task_inputs)
        if isawaitable(task_res):
            task_res = await task_res
    except TaskCancelled:
        worker.info('Task(id="%s") cancelled during execution')
        return
    except recoverable_errors as e:
        # This will throw a MaxRetriesExceeded when necessary
        worker.check_retries(retries, task)
        error = TaskError.from_exception(e)
        await worker.publish_error_event(
            error=error,
            task_id=task.id,
            project=project,
            retries=retries + 1,
        )
        raise RecoverableError() from e
    worker.info('Task(id="%s") complete, saving result...', task.id)
    result = TaskResult(task_id=task.id, result=task_res)
    await worker.save_result(result, project)
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


def _format_error(error: Exception) -> str:
    return "".join(traceback.format_exception(None, error, error.__traceback__))
