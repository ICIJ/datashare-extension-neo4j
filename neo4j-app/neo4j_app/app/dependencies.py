import logging
import multiprocessing
import os
import tempfile
from contextlib import asynccontextmanager
from multiprocessing.managers import SyncManager
from pathlib import Path
from typing import Optional, cast

import neo4j
from fastapi import FastAPI

from neo4j_app.app.config import ServiceConfig
from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.icij_worker import (
    EventPublisher,
    Neo4jEventPublisher,
)
from neo4j_app.icij_worker.backend.backend import WorkerBackend
from neo4j_app.icij_worker.task_manager import TaskManager
from neo4j_app.icij_worker.task_manager.neo4j import Neo4JTaskManager
from neo4j_app.icij_worker.utils import run_deps
from neo4j_app.icij_worker.utils.dependencies import DependencyInjectionError
from neo4j_app.tasks.dependencies import (
    config_enter,
    create_project_registry_db_enter,
    es_client_enter,
    es_client_exit,
    lifespan_config,
    lifespan_neo4j_driver,
    migrate_app_db_enter,
    neo4j_driver_enter,
    neo4j_driver_exit,
)

logger = logging.getLogger(__name__)

_ASYNC_APP_CONFIG_PATH: Optional[Path] = None
_ES_CLIENT: Optional[ESClientABC] = None
_EVENT_PUBLISHER: Optional[EventPublisher] = None
_MP_CONTEXT = None
_NEO4J_DRIVER: Optional[neo4j.AsyncDriver] = None
_PROCESS_MANAGER: Optional[SyncManager] = None
_TASK_MANAGER: Optional[TaskManager] = None
_TEST_DB_FILE: Optional[Path] = None
_TEST_LOCK: Optional[multiprocessing.Lock] = None
_WORKER_POOL_IS_RUNNING = False


def write_async_app_config_enter(**_):
    config = lifespan_config()
    config = cast(ServiceConfig, config)
    global _ASYNC_APP_CONFIG_PATH
    _, _ASYNC_APP_CONFIG_PATH = tempfile.mkstemp(
        prefix="neo4j-worker-config", suffix=".properties"
    )
    _ASYNC_APP_CONFIG_PATH = Path(_ASYNC_APP_CONFIG_PATH)
    with _ASYNC_APP_CONFIG_PATH.open("w") as f:
        config.write_java_properties(file=f)
    logger.info("Loaded config %s", config.json(indent=2))


def write_async_app_config_exit(*_, **__):
    path = _lifespan_async_app_config_path()
    if path.exists():
        os.remove(path)


def _lifespan_async_app_config_path() -> Path:
    if _ASYNC_APP_CONFIG_PATH is None:
        raise DependencyInjectionError("async app config path")
    return _ASYNC_APP_CONFIG_PATH


def loggers_enter(**_):
    config = lifespan_config()
    config.setup_loggers()
    logger.info("Loggers ready to log 💬")


def mp_context_enter(**__):
    global _MP_CONTEXT
    _MP_CONTEXT = multiprocessing.get_context("spawn")


def lifespan_mp_context():
    if _MP_CONTEXT is None:
        raise DependencyInjectionError("multiprocessing context")
    return _MP_CONTEXT


def test_db_path_enter(**_):
    config = cast(
        ServiceConfig,
        lifespan_config(),
    )
    if config.test:
        # pylint: disable=consider-using-with
        from neo4j_app.tests.icij_worker.conftest import DBMixin

        global _TEST_DB_FILE
        _TEST_DB_FILE = tempfile.NamedTemporaryFile(prefix="db", suffix=".json")

        DBMixin.fresh_db(Path(_TEST_DB_FILE.name))
        _TEST_DB_FILE.__enter__()  # pylint: disable=unnecessary-dunder-call


def test_db_path_exit(exc_type, exc_value, trace):
    if _TEST_DB_FILE is not None:
        _TEST_DB_FILE.__exit__(exc_type, exc_value, trace)


def _lifespan_test_db_path() -> Path:
    if _TEST_DB_FILE is None:
        raise DependencyInjectionError("test db path")
    return Path(_TEST_DB_FILE.name)


def test_process_manager_enter(**_):
    global _PROCESS_MANAGER
    _PROCESS_MANAGER = lifespan_mp_context().Manager()


def test_process_manager_exit(exc_type, exc_value, trace):
    _PROCESS_MANAGER.__exit__(exc_type, exc_value, trace)


def lifespan_test_process_manager() -> SyncManager:
    if _PROCESS_MANAGER is None:
        raise DependencyInjectionError("process manager")
    return _PROCESS_MANAGER


def _test_lock_enter(**_):
    config = cast(
        ServiceConfig,
        lifespan_config(),
    )
    if config.test:
        global _TEST_LOCK
        _TEST_LOCK = lifespan_test_process_manager().Lock()


def _lifespan_test_lock() -> multiprocessing.Lock:
    if _TEST_LOCK is None:
        raise DependencyInjectionError("test lock")
    return cast(multiprocessing.Lock, _TEST_LOCK)


def lifespan_worker_pool_is_running() -> bool:
    return _WORKER_POOL_IS_RUNNING


def task_manager_enter(**_):
    global _TASK_MANAGER
    config = cast(
        ServiceConfig,
        lifespan_config(),
    )
    if config.test:
        from neo4j_app.tests.icij_worker.conftest import MockManager

        _TASK_MANAGER = MockManager(
            _lifespan_test_db_path(),
            _lifespan_test_lock(),
            max_queue_size=config.neo4j_app_task_queue_size,
        )
    else:
        _TASK_MANAGER = Neo4JTaskManager(
            lifespan_neo4j_driver(), max_queue_size=config.neo4j_app_task_queue_size
        )


def lifespan_task_manager() -> TaskManager:
    if _TASK_MANAGER is None:
        raise DependencyInjectionError("task manager")
    return cast(TaskManager, _TASK_MANAGER)


def event_publisher_enter(**_):
    global _EVENT_PUBLISHER
    config = cast(
        ServiceConfig,
        lifespan_config(),
    )
    if config.test:
        from neo4j_app.tests.icij_worker.conftest import MockEventPublisher

        _EVENT_PUBLISHER = MockEventPublisher(
            _lifespan_test_db_path(), _lifespan_test_lock()
        )
    else:
        _EVENT_PUBLISHER = Neo4jEventPublisher(lifespan_neo4j_driver())


def lifespan_event_publisher() -> EventPublisher:
    if _EVENT_PUBLISHER is None:
        raise DependencyInjectionError("event publisher")
    return cast(EventPublisher, _EVENT_PUBLISHER)


@asynccontextmanager
async def run_app_deps(app: FastAPI):
    config = app.state.config
    n_workers = config.neo4j_app_n_async_workers
    async with run_deps(
        dependencies=FASTAPI_LIFESPAN_DEPS, ctx="FastAPI HTTP server", config=config
    ):
        app.state.config = await config.with_neo4j_support()
        worker_extras = {"teardown_dependencies": config.test}
        config_extra = dict()
        # Forward the past of the app config to load to the async app
        async_app_extras = {"config_path": _lifespan_async_app_config_path()}
        if config.test:
            config_extra["db_path"] = _lifespan_test_db_path()
            worker_extras["lock"] = _lifespan_test_lock()
        worker_config = config.to_worker_config(**config_extra)
        with WorkerBackend.MULTIPROCESSING.run_cm(
            config.neo4j_app_async_app,
            n_workers=n_workers,
            config=worker_config,
            worker_extras=worker_extras,
            app_deps_extras=async_app_extras,
        ):
            global _WORKER_POOL_IS_RUNNING
            _WORKER_POOL_IS_RUNNING = True
            yield
        _WORKER_POOL_IS_RUNNING = False


FASTAPI_LIFESPAN_DEPS = [
    ("configuration reading", config_enter, None),
    ("loggers setup", loggers_enter, None),
    (
        "write async config for workers",
        write_async_app_config_enter,
        write_async_app_config_exit,
    ),
    ("neo4j driver creation", neo4j_driver_enter, neo4j_driver_exit),
    ("neo4j project registry creation", create_project_registry_db_enter, None),
    ("ES client creation", es_client_enter, es_client_exit),
    (None, mp_context_enter, None),
    (None, test_process_manager_enter, test_process_manager_exit),
    (None, test_db_path_enter, test_db_path_exit),
    (None, _test_lock_enter, None),
    ("task manager creation", task_manager_enter, None),
    ("event publisher creation", event_publisher_enter, None),
    ("neo4j DB migration", migrate_app_db_enter, None),
]
