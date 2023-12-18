import functools
import signal
import sys
from abc import ABC

from neo4j_app.icij_worker.worker.worker import Worker

_HANDLE_SIGNALS = [
    signal.SIGINT,
    signal.SIGTERM,
]
if sys.platform == "win32":
    _HANDLE_SIGNALS += [signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT]


class ProcessWorkerMixin(Worker, ABC):
    async def _aenter__(self):
        await super()._aenter__()
        self._setup_signal_handlers()

    def _signal_handler(self, signal_name: signal.Signals, *, graceful: bool):
        self.error("received %s", signal_name)
        self._graceful_shutdown = graceful
        if self._work_forever_task is not None:
            self.info("cancelling worker loop...")
            self._work_forever_task.cancel()

    def _setup_signal_handlers(self):
        # Let's always shutdown gracefully for now since when the server shutdown
        # it will try to SIGTERM, we want to avoid loosing track of running tasks
        for s in _HANDLE_SIGNALS:
            handler = functools.partial(self._signal_handler, s, graceful=True)
            self._loop.add_signal_handler(s, handler)
