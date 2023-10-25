import functools
import signal
from abc import ABC
from typing import Callable, cast

from neo4j_app.icij_worker.exceptions import WorkerCancelled
from neo4j_app.icij_worker.worker.worker import Worker


class ProcessWorkerMixin(Worker, ABC):
    async def _aenter__(self):
        await super()._aenter__()
        self._setup_signal_handlers()

    def _signal_handler(
        self,
        signal_name: int,
        _,
        __,  # pylint: disable=invalid-name
        *,
        graceful: bool,
    ):
        if not self._already_shutdown:
            self.error("received %s", signal_name)
            self._graceful_shutdown = graceful
            raise WorkerCancelled()

    def _setup_signal_handlers(self):
        # Let's always shutdown gracefully for now since when the server shutdown
        # it will try to SIGTERM, we want to avoid loosing track of running tasks
        for s in ["SIGINT", "SIGTERM", "CTRL_C_EVENT", "CTRL_BREAK_EVENT"]:
            try:
                signalnum = getattr(signal, s)
            except AttributeError:
                continue
            handler = functools.partial(self._signal_handler, s, graceful=True)
            handler = cast(Callable[[int], None], handler)
            signal.signal(signalnum, handler)
