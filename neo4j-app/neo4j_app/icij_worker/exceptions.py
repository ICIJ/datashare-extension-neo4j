import abc
from typing import Optional, Sequence


class ICIJWorkerError(metaclass=abc.ABCMeta):
    ...


class MaxRetriesExceeded(ICIJWorkerError, RuntimeError):
    ...


class RecoverableError(ICIJWorkerError, Exception):
    ...


class UnknownTask(ICIJWorkerError, ValueError):
    def __init__(self, task_id: str, worker_id: Optional[str] = None):
        msg = f'Unknown task "{task_id}"'
        if worker_id is not None:
            msg += f" for worker {worker_id}"
        super().__init__(msg)


class TaskQueueIsFull(ICIJWorkerError, RuntimeError):
    def __init__(self, max_queue_size: int):
        super().__init__(f"task queue is full ({max_queue_size}/{max_queue_size})")


class TaskCancelled(ICIJWorkerError, RuntimeError):
    def __init__(self, task_id: str):
        super().__init__(f'Task(id="{task_id}") has been cancelled')


class TaskAlreadyExists(ICIJWorkerError, ValueError):
    def __init__(self, task_id: Optional[str] = None):
        msg = f'task "{task_id}" already exists'
        super().__init__(msg)


class TaskAlreadyReserved(ICIJWorkerError, ValueError):
    def __init__(self, task_id: Optional[str] = None):
        msg = "task "
        if task_id is not None:
            msg += f'"{task_id}" '
        msg += "is already reserved"
        super().__init__(msg)


class UnregisteredTask(ICIJWorkerError, ValueError):
    def __init__(self, task_name: str, available_tasks: Sequence[str], *args, **kwargs):
        msg = (
            f'UnregisteredTask task "{task_name}", available tasks: {available_tasks}. '
            f"Task must be registered using the @task decorator."
        )
        super().__init__(msg, *args, **kwargs)


class MissingTaskResult(ICIJWorkerError, LookupError):
    def __init__(self, task_id: str):
        msg = f'Result of task "{task_id}" couldn\'t be found, did it complete ?'
        super().__init__(msg)
