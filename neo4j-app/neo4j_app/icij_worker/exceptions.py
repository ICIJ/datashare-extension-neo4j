import abc
from typing import Sequence


class ICIJWorkerError(metaclass=abc.ABCMeta):
    ...


class MaxRetriesExceeded(ICIJWorkerError, RuntimeError):
    ...


class UnknownTask(ICIJWorkerError, ValueError):
    def __init__(self, task_id: str):
        super().__init__(f'Unknown task "{task_id}"')


class TaskAlreadyReserved(ICIJWorkerError, ValueError):
    def __init__(self, task_id: str):
        super().__init__(f'task "{task_id}" is already reserved')


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
