import functools
import signal
import threading
from abc import ABC
from multiprocessing import Queue
from typing import Callable, Tuple, cast

from neo4j_app.icij_worker import ICIJApp, Task
from neo4j_app.icij_worker.worker.worker import Worker


class ProcessWorkerMixin(Worker, ABC):
    def __init__(self, app: ICIJApp, worker_id: str, queue: Queue):
        super().__init__(app, worker_id)
        self._queue = queue

    async def receive(self) -> Tuple[Task, str]:
        task, project = self._queue.get(block=True, timeout=None)
        return task, project

    async def __aenter__(self):
        self._setup_signal_handlers()

    def _signal_handler(
        self,
        signal_name: int,
        _,
        __,  # pylint: disable=invalid-name
        *,
        graceful: bool,
    ):
        self.error("received %s", signal_name)
        self._graceful_shutdown = graceful
        raise KeyboardInterrupt()

    def _setup_signal_handlers(self):
        if threading.current_thread() is threading.main_thread():
            handle_sigint = functools.partial(
                self._signal_handler, "SIGINT", graceful=True
            )
            handle_sigint = cast(Callable[[int], None], handle_sigint)
            signal.signal(signal.SIGINT, handle_sigint)
            handle_sigterm = functools.partial(
                self._signal_handler, "SIGTERM", graceful=False
            )
            handle_sigterm = cast(Callable[[int], None], handle_sigterm)
            signal.signal(signal.SIGTERM, handle_sigterm)
