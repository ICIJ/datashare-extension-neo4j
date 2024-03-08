import logging
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

from neo4j_app.icij_worker import WorkerConfig
from neo4j_app.icij_worker.backend.mp import (
    run_workers_with_multiprocessing,
    run_workers_with_multiprocessing_cm,
)

logger = logging.getLogger(__name__)


class WorkerBackend(str, Enum):
    # pylint: disable=invalid-name

    # We could support more backend type, and for instance support asyncio/thread backed
    # workers for IO based tasks
    MULTIPROCESSING = "multiprocessing"

    # TODO: refactor this one to be a function rather than a cm coroutine a context
    #  manager is no longer needed to run workers inside the HTTP server
    @contextmanager
    def run(
        self,
        app: str,
        n_workers: int,
        config: WorkerConfig,
        worker_extras: Optional[Dict] = None,
        app_deps_extras: Optional[Dict] = None,
    ):
        run_workers_with_multiprocessing(
            app,
            n_workers,
            config,
            worker_extras=worker_extras,
            app_deps_extras=app_deps_extras,
        )

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
        app_deps_extras: Optional[Dict] = None,
    ):
        if self is WorkerBackend.MULTIPROCESSING:
            with run_workers_with_multiprocessing_cm(
                app,
                n_workers,
                config,
                worker_extras=worker_extras,
                app_deps_extras=app_deps_extras,
            ):
                yield
        else:
            raise NotImplementedError(f"Can't start workers with backend: {self}")


def start_workers(
    app: str, n_workers: int, config_path: Optional[Path], backend: WorkerBackend
):
    if n_workers < 1:
        raise ValueError("n_workers must be >= 1")
    if config_path is not None:
        logger.info("Loading worker configuration from %s", config_path)
        config = WorkerConfig.parse_file(config_path)
    else:
        logger.info("Loading worker configuration from env...")
        config = WorkerConfig.from_env()

    backend.run(app, n_workers=n_workers, config=config)
