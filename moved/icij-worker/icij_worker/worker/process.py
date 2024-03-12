import asyncio
import functools
import logging
import signal
from abc import ABC
from asyncio import AbstractEventLoop
from typing import Optional
from icij_common.logging_utils import LogWithNameMixin


_HANDLE_SIGNALS = [signal.SIGTERM]


# TODO: rename this file to signals
class HandleSignalsMixin(LogWithNameMixin, ABC):
    _work_forever_task: Optional[asyncio.Task]
    _loop: AbstractEventLoop

    def __init__(self, logger: logging.Logger, handle_signals: bool = True):
        super().__init__(logger)
        self._handle_signals = handle_signals

    async def _aenter__(self):
        # TODO: define this one on the worker side
        if self._handle_signals:
            self._setup_child_process_signal_handlers()

    def _signal_handler(self, signal_name: signal.Signals, *, graceful: bool):
        self.error("received %s", signal_name)
        self._graceful_shutdown = graceful
        if self._work_forever_task is not None:
            self.info("cancelling worker loop...")
            self._work_forever_task.cancel()

    def _setup_child_process_signal_handlers(self):
        # We ignore SIGINT (graceful shutdown), this signal is handled by the
        # process handling the pool, which will terminate the pool and send a SIGTERM,
        # which is handled here

        self._loop.add_signal_handler(signal.SIGINT, signal.getsignal(signal.SIG_IGN))
        for s in _HANDLE_SIGNALS:
            handler = functools.partial(self._signal_handler, s, graceful=True)
            self._loop.add_signal_handler(s, handler)
