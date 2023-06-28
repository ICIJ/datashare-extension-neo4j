import abc
from typing import Sequence


class ICIJWorkerError(metaclass=abc.ABCMeta):
    ...


class MaxReconnectionExceeded(ICIJWorkerError, ConnectionError):
    ...


class MaxRetriesExceeded(ICIJWorkerError, RuntimeError):
    ...


class ConnectionLostError(ICIJWorkerError, ConnectionError):
    ...


class InvalidTaskBody(ICIJWorkerError, ValueError):
    ...


class UnregisteredTask(ICIJWorkerError, ValueError):
    def __init__(self, task_name: str, available_tasks: Sequence[str], *args, **kwargs):
        msg = (
            f'UnregisteredTask task "{task_name}", available tasks: {available_tasks}. '
            f"Task must be registered using the @task decorator."
        )
        super().__init__(msg, *args, **kwargs)
