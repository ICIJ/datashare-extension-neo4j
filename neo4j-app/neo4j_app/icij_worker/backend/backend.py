from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

from neo4j_app.icij_worker import WorkerConfig
from neo4j_app.icij_worker.backend.mp import run_workers_with_multiprocessing


class WorkerBackend(str, Enum):
    # pylint: disable=invalid-name

    # We could support more backend type, and for instance support asyncio/thread backed
    # workers for IO based tasks
    MULTIPROCESSING = "multiprocessing"

    def run(
        self,
        app: str,
        n_workers: int,
        config: WorkerConfig,
        worker_extras: Optional[Dict] = None,
    ):
        # This function is meant to be run as the main function of a Python command,
        # in this case we want th main process to handle signals
        with self._run_cm(
            app,
            n_workers,
            config,
            handle_signals=True,
            worker_extras=worker_extras,
        ):
            pass

    # TODO: remove this when the HTTP server doesn't
    # TODO: also refactor underlying functions to be simple function rather than
    #  context managers
    @contextmanager
    def run_cm(
        self,
        app: str,
        n_workers: int,
        config: WorkerConfig,
        worker_extras: Optional[Dict] = None,
    ):
        # This usage is meant for when a backend is run from another process which
        # handles signals by itself
        with self._run_cm(
            app,
            n_workers,
            config,
            handle_signals=False,
            worker_extras=worker_extras,
        ):
            yield

    @contextmanager
    def _run_cm(
        self,
        app: str,
        n_workers: int,
        config: WorkerConfig,
        *,
        handle_signals: bool = False,
        worker_extras: Optional[Dict] = None,
    ):
        if self is WorkerBackend.MULTIPROCESSING:
            with run_workers_with_multiprocessing(
                app,
                n_workers,
                config,
                handle_signals=handle_signals,
                worker_extras=worker_extras,
            ):
                yield
        else:
            raise NotImplementedError(f"Can't start workers with backend: {self}")


def start_workers(
    app: str,
    n_workers: int,
    config_path: Optional[Path],
    backend: WorkerBackend,
):
    if n_workers < 1:
        raise ValueError("n_workers must be >= 1")
    if config_path is not None:
        config = WorkerConfig.parse_file(config_path)
    else:
        config = WorkerConfig()
    backend.run(app, n_workers=n_workers, config=config)
