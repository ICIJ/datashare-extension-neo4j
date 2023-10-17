import functools
import signal
import threading
from abc import ABC
from typing import Callable, cast

from neo4j_app.icij_worker.worker.worker import Worker


class ProcessWorkerMixin(Worker, ABC):
    async def __aenter__(self):
        await super().__aenter__()
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
