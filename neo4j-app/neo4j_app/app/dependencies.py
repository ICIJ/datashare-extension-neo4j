import logging
import multiprocessing
import os
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Optional, cast

import neo4j
from fastapi import FastAPI

from neo4j_app.app.config import ServiceConfig
from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.icij_worker import (
    EventPublisher,
    Neo4jEventPublisher,
    WorkerConfig,
)
from neo4j_app.icij_worker.backend.backend import WorkerBackend
from neo4j_app.icij_worker.task_manager import TaskManager
from neo4j_app.icij_worker.task_manager.neo4j import Neo4JTaskManager
from neo4j_app.icij_worker.utils import run_deps
from neo4j_app.icij_worker.utils.dependencies import DependencyInjectionError
from neo4j_app.icij_worker.utils.imports import import_variable
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
_TASK_MANAGER: Optional[TaskManager] = None
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


def lifespan_worker_pool_is_running() -> bool:
    return _WORKER_POOL_IS_RUNNING


def task_manager_enter(**_):
    global _TASK_MANAGER
    config = cast(ServiceConfig, lifespan_config())
    _TASK_MANAGER = Neo4JTaskManager(
        lifespan_neo4j_driver(), max_queue_size=config.neo4j_app_task_queue_size
    )


def lifespan_task_manager() -> TaskManager:
    if _TASK_MANAGER is None:
        raise DependencyInjectionError("task manager")
    return cast(TaskManager, _TASK_MANAGER)


def event_publisher_enter(**_):
    global _EVENT_PUBLISHER
    _EVENT_PUBLISHER = Neo4jEventPublisher(lifespan_neo4j_driver())


def lifespan_event_publisher() -> EventPublisher:
    if _EVENT_PUBLISHER is None:
        raise DependencyInjectionError("event publisher")
    return cast(EventPublisher, _EVENT_PUBLISHER)


@asynccontextmanager
async def run_http_service_deps(
    app: FastAPI,
    async_app: str,
    worker_config: WorkerConfig,
    worker_extras: Optional[Dict] = None,
):
    config = app.state.config
    n_workers = config.neo4j_app_n_async_workers
    deps = import_variable(config.neo4j_app_dependencies)
    async with run_deps(dependencies=deps, ctx="FastAPI HTTP server", config=config):
        # Compute the support only once we know the neo4j driver deps has successfully
        # completed
        app.state.config = await config.with_neo4j_support()
        # config_extra = dict()
        # # Forward the part of the app config to load to the async app
        # async_app_extras = {"config_path": _lifespan_async_app_config_path()}
        # if is_test:
        #     config_extra["db_path"] = _lifespan_test_db_path()
        # TODO 1: set the async app config path inside the deps itself
        # TODO 3: set the DB path in deps
        with WorkerBackend.MULTIPROCESSING.run_cm(
            async_app,
            n_workers=n_workers,
            config=worker_config,
            worker_extras=worker_extras,
        ):
            global _WORKER_POOL_IS_RUNNING
            _WORKER_POOL_IS_RUNNING = True
            yield
        _WORKER_POOL_IS_RUNNING = False


HTTP_SERVICE_LIFESPAN_DEPS = [
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
    ("task manager creation", task_manager_enter, None),
    ("event publisher creation", event_publisher_enter, None),
    ("neo4j DB migration", migrate_app_db_enter, None),
]
