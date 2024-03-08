from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import traceback
from abc import abstractmethod
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
    TypeVar,
    final,
)

from neo4j_app.core.utils.progress import CheckCancelledProgress
from neo4j_app.icij_worker.app import AsyncApp, RegisteredTask
from neo4j_app.icij_worker.event_publisher import EventPublisher
from neo4j_app.icij_worker.exceptions import (
    MaxRetriesExceeded,
    RecoverableError,
    TaskAlreadyReserved,
    TaskCancelled,
    UnregisteredTask,
)
from neo4j_app.icij_worker.task import (
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from neo4j_app.icij_worker.utils.registrable import Registrable
from neo4j_app.icij_worker.worker.process import HandleSignalsMixin
from neo4j_app.typing_ import PercentProgress

logger = logging.getLogger(__name__)

PROGRESS_HANDLER_ARG = "progress"

C = TypeVar("C", bound="WorkerConfig")


class Worker(
    EventPublisher,
    Registrable,
    HandleSignalsMixin,
    AbstractAsyncContextManager,
):
    def __init__(
        self,
        app: AsyncApp,
        worker_id: str,
        handle_signals: bool = True,
        teardown_dependencies: bool = False,
    ):
        # If worker are run using a thread backend then signal handling might not be
        # required, in this case the signal handling mixing will just do nothing
        HandleSignalsMixin.__init__(self, logger, handle_signals=handle_signals)
        self._app = app
        self._id = worker_id
        self._teardown_dependencies = teardown_dependencies
        self._graceful_shutdown = True
        self._loop = asyncio.get_event_loop()
        self._work_forever_task: Optional[asyncio.Task] = None
        self._already_exiting = False
        self._current = None
        self._cancelled_ = defaultdict(set)
        self._config: Optional[C] = None

    def set_config(self, config: C):
        self._config = config

    def _to_config(self) -> C:
        if self._config is None:
            raise ValueError(
                "worker was initialized using a from_config, "
                "but the config was not attached using .set_config"
            )
        return self._config

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def _cancelled(self) -> List[str]:
        return list(self._cancelled_)

    @functools.cached_property
    def id(self) -> str:
        return self._id

    @final
    def work_forever(self):
        with self:  # The graceful shutdown happens here
            self.info("started working...")
            self._work_forever_task = self._loop.create_task(self._work_forever())
            try:
                self._loop.run_until_complete(self._work_forever_task)
            except asyncio.CancelledError:  # Shutdown let's not reraise
                self.info("worker cancelled, shutting down...")
            except KeyboardInterrupt:  # Shutdown let's not reraise
                pass
            except Exception as e:
                self.error("error occurred while consuming: %s", _format_error(e))
                self.info("will try to shutdown gracefully...")
                raise e
        self.info(
            "finally stopped working, nothing lasts forever, "
            "i'm out of this busy life !"
        )

    @final
    async def _work_forever(self):
        while True:
            await self._work_once()

    @final
    def logged_name(self) -> str:
        return self.id

    @property
    def graceful_shutdown(self) -> bool:
        return self._graceful_shutdown

    @final
    async def _work_once(self):
        await task_wrapper(self)

    @final
    async def consume(self) -> Tuple[Task, str]:
        return await self._consume()

    @final
    @asynccontextmanager
    async def acknowledgment_cm(self, task: Task, project: str):
        try:
            self._current = task, project
            self.debug('Task(id="%s") locked', task.id)
            event = TaskEvent(task_id=task.id, progress=0, status=TaskStatus.RUNNING)
            await self.publish_event(event, project)
            yield
            await self.acknowledge(task, project)
            self.info('Task(id="%s") successful !', task.id)
        except asyncio.CancelledError as e:
            self.error(
                'Task(id="%s") worker cancelled, exiting without persisting error',
                task.id,
            )
            raise e
        except RecoverableError:
            self.error('Task(id="%s") encountered error', task.id)
            await self.negatively_acknowledge(task, project, requeue=True)
        except Exception as fatal_error:  # pylint: disable=broad-exception-caught
            if isinstance(fatal_error, MaxRetriesExceeded):
                self.error('Task(id="%s") exceeded max retries, exiting !', task.id)
            else:
                self.error('Task(id="%s") fatal error, exiting !', task.id)
            task_error = TaskError.from_exception(fatal_error)
            await self.save_error(error=task_error, task=task, project=project)
            await self.negatively_acknowledge(task, project, requeue=False)

    @final
    async def acknowledge(self, task: Task, project: str):
        completed_at = datetime.now()
        self.info('Task(id="%s") acknowledging...', task.id)
        await self._acknowledge(task, project, completed_at)
        self._current = None
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
            "negatively acknowledging Task(id=%s) with (requeue=%s)...",
            task.id,
            requeue,
        )
        nacked = await self._negatively_acknowledge(task, project, requeue=requeue)
        self._current = None
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
        self.debug('Task(id="%s") saving error', task.id)
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
        return self.config.cancelled_tasks_refresh_interval_s

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
    def __enter__(self):
        self._loop.run_until_complete(self.__aenter__())

    @final
    async def __aenter__(self):
        await self._aenter__()

    async def _aenter__(self):
        pass

    @final
    def __exit__(self, exc_type, exc_value, tb):
        self._loop.run_until_complete(self.__aexit__(exc_type, exc_value, tb))

    @final
    async def __aexit__(self, exc_type, exc_value, tb):
        # dependencies might be closed while trying to gracefully shutdown
        if not self._already_exiting:
            self._already_exiting = True
            # Let's try to shut down gracefully
            await self.shutdown()
            # Clean worker dependencies only if needed, dependencies might be share in
            # which case we don't want to tear them down
            if self._teardown_dependencies:
                self.info("cleaning worker dependencies...")
                await self._aexit__(exc_type, exc_value, tb)

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @final
    async def _shutdown_gracefully(self):
        await self._negatively_acknowledge_running_task(requeue=True)

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
            self.info("shutting down the hard way, task might not be re-queued...")


def _retrieve_registered_task(
    task: Task,
    app: AsyncApp,
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
