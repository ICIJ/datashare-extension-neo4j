import inspect
import logging
import multiprocessing
import os
import sys
import tempfile
import traceback
from contextlib import asynccontextmanager, contextmanager
from multiprocessing.managers import SyncManager
from pathlib import Path
from typing import AsyncGenerator, Optional, cast

import neo4j
from fastapi import FastAPI

from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.neo4j import MIGRATIONS, migrate_db_schemas
from neo4j_app.core.neo4j.migrations import delete_all_migrations
from neo4j_app.core.neo4j.projects import create_project_registry_db
from neo4j_app.icij_worker import (
    EventPublisher,
    Neo4jEventPublisher,
)
from neo4j_app.icij_worker.task_store import TaskStore
from neo4j_app.icij_worker.task_store.neo4j import Neo4jTaskStore

logger = logging.getLogger(__name__)

_CONFIG: Optional[AppConfig] = None
_ES_CLIENT: Optional[ESClientABC] = None
_EVENT_PUBLISHER: Optional[EventPublisher] = None
_PROCESS_MANAGER: Optional[SyncManager] = None
_NEO4J_DRIVER: Optional[neo4j.AsyncDriver] = None
_TASK_STORE: Optional[TaskStore] = None
_TEST_DB_FILE: Optional[Path] = None
_TEST_LOCK: Optional[multiprocessing.Lock] = None
_WORKER_POOL: Optional[multiprocessing.Pool] = None


class DependencyInjectionError(RuntimeError):
    def __init__(self, name: str):
        msg = f"{name} was not injected"
        super().__init__(msg)


def config_enter(config: AppConfig):
    global _CONFIG
    _CONFIG = config


def loggers_enter(_: AppConfig):
    config = lifespan_config()
    config.setup_loggers()
    logger.info("Loggers ready to log 💬")


def lifespan_config() -> AppConfig:
    if _CONFIG is None:
        raise DependencyInjectionError("config")
    return cast(AppConfig, _CONFIG)


async def neo4j_driver_enter(_: AppConfig):
    from neo4j_app.core.neo4j.projects import registry_db_session

    global _NEO4J_DRIVER
    _NEO4J_DRIVER = lifespan_config().to_neo4j_driver()
    await _NEO4J_DRIVER.__aenter__()  # pylint: disable=unnecessary-dunder-call

    logger.debug("pinging neo4j...")
    async with registry_db_session(_NEO4J_DRIVER) as sess:
        await sess.run("CALL db.ping()")
    logger.debug("neo4j driver is ready")


async def neo4j_driver_exit(exc_type, exc_value, trace):
    await _NEO4J_DRIVER.__aexit__(exc_type, exc_value, trace)


def lifespan_neo4j_driver() -> neo4j.AsyncDriver:
    if _NEO4J_DRIVER is None:
        raise DependencyInjectionError("neo4j driver")
    return cast(neo4j.AsyncDriver, _NEO4J_DRIVER)


async def es_client_enter(_: AppConfig):
    global _ES_CLIENT
    _ES_CLIENT = lifespan_config().to_es_client()
    await _ES_CLIENT.__aenter__()  # pylint: disable=unnecessary-dunder-call


async def es_client_exit(exc_type, exc_value, trace):
    await _ES_CLIENT.__aexit__(exc_type, exc_value, trace)


def lifespan_es_client() -> ESClientABC:
    if _ES_CLIENT is None:
        raise DependencyInjectionError("es client")
    return cast(ESClientABC, _ES_CLIENT)


def test_db_path_enter(_: AppConfig):
    config = lifespan_config()
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


def test_process_manager_enter(_: AppConfig):
    global _PROCESS_MANAGER
    _PROCESS_MANAGER = multiprocessing.Manager()


def test_process_manager_exit(exc_type, exc_value, trace):
    _PROCESS_MANAGER.__exit__(exc_type, exc_value, trace)


def lifespan_test_process_manager() -> SyncManager:
    if _PROCESS_MANAGER is None:
        raise DependencyInjectionError("process manager")
    return _PROCESS_MANAGER


def _test_lock_enter(_: AppConfig):
    config = lifespan_config()
    if config.test:
        global _TEST_LOCK
        _TEST_LOCK = lifespan_test_process_manager().Lock()


def _lifespan_test_lock() -> multiprocessing.Lock:
    if _TEST_LOCK is None:
        raise DependencyInjectionError("test lock")
    return cast(multiprocessing.Lock, _TEST_LOCK)


def worker_pool_enter(_: AppConfig):
    # pylint: disable=consider-using-with
    config = lifespan_config()
    global _WORKER_POOL
    _WORKER_POOL = multiprocessing.Pool(processes=config.neo4j_app_n_async_workers)
    _WORKER_POOL.__enter__()  # pylint: disable=unnecessary-dunder-call
    process_id = os.getpid()
    config = lifespan_config()
    n_workers = min(config.neo4j_app_n_async_workers, config.neo4j_app_task_queue_size)
    worker_ids = [f"worker-{process_id}-{i}" for i in range(n_workers)]

    kwargs = dict()
    worker_cls = config.to_worker_cls()
    if worker_cls.__name__ == "MockWorker":
        kwargs = {"db_path": _lifespan_test_db_path(), "lock": _lifespan_test_lock()}

    kwargs["config"] = config
    for w_id in worker_ids:
        kwargs.update({"worker_id": w_id})
        logger.info("starting worker %s", w_id)
        _WORKER_POOL.apply_async(worker_cls.work_forever_from_config, kwds=kwargs)

    logger.info("worker pool ready !")


def worker_pool_exit(exc_type, exc_value, trace):
    # pylint: disable=unused-argument
    pool = _lifespan_worker_pool()
    pool.__exit__(exc_type, exc_value, trace)


def _lifespan_worker_pool() -> multiprocessing.Pool:
    if _WORKER_POOL is None:
        raise DependencyInjectionError("worker pool")
    return cast(multiprocessing.Pool, _WORKER_POOL)


def task_store_enter(_: AppConfig):
    global _TASK_STORE
    config = lifespan_config()
    if config.test:
        from neo4j_app.tests.icij_worker.conftest import MockStore

        _TASK_STORE = MockStore(
            _lifespan_test_db_path(),
            _lifespan_test_lock(),
            max_queue_size=config.neo4j_app_task_queue_size,
        )
    else:
        _TASK_STORE = Neo4jTaskStore(
            lifespan_neo4j_driver(), max_queue_size=config.neo4j_app_task_queue_size
        )


def lifespan_task_store() -> TaskStore:
    if _TASK_STORE is None:
        raise DependencyInjectionError("task store")
    return cast(TaskStore, _TASK_STORE)


def event_publisher_enter(_: AppConfig):
    global _EVENT_PUBLISHER
    config = lifespan_config()
    if config.test:
        from neo4j_app.tests.icij_worker.conftest import MockEventPublisher

        _EVENT_PUBLISHER = MockEventPublisher(
            _lifespan_test_db_path(), _lifespan_test_lock()
        )
    else:
        _EVENT_PUBLISHER = Neo4jEventPublisher(lifespan_neo4j_driver())


async def create_project_registry_db_enter(_: AppConfig):
    driver = lifespan_neo4j_driver()
    await create_project_registry_db(driver)


async def migrate_app_db_enter(config: AppConfig):
    logger.info("Running schema migrations...")
    driver = lifespan_neo4j_driver()
    if config.force_migrations:
        # TODO: improve this as is could lead to race conditions...
        await delete_all_migrations(driver)
    await migrate_db_schemas(
        driver,
        registry=MIGRATIONS,
        timeout_s=config.neo4j_app_migration_timeout_s,
        throttle_s=config.neo4j_app_migration_throttle_s,
    )


def lifespan_event_publisher() -> EventPublisher:
    if _EVENT_PUBLISHER is None:
        raise DependencyInjectionError("event publisher")
    return cast(EventPublisher, _EVENT_PUBLISHER)


FASTAPI_LIFESPAN_DEPS = [
    (config_enter, None),
    (loggers_enter, None),
    (neo4j_driver_enter, neo4j_driver_exit),
    (es_client_enter, es_client_exit),
    (test_process_manager_enter, test_process_manager_exit),
    (test_db_path_enter, test_db_path_exit),
    (_test_lock_enter, None),
    (task_store_enter, None),
    (event_publisher_enter, None),
    (worker_pool_enter, worker_pool_exit),
    (create_project_registry_db_enter, None),
    (migrate_app_db_enter, None),
]


@contextmanager
def _log_and_reraise():
    try:
        yield
    except Exception as exc:
        from neo4j_app.app.utils import INTERNAL_SERVER_ERROR

        title = INTERNAL_SERVER_ERROR
        detail = f"{type(exc).__name__}: {exc}"
        trace = "".join(traceback.format_exc())
        logger.error("%s\nDetail: %s\nTrace: %s", title, detail, trace)

        raise exc


@asynccontextmanager
async def run_app_deps(app: FastAPI, dependencies) -> AsyncGenerator[None, None]:
    async with run_deps(app.state.config, dependencies):
        yield


@asynccontextmanager
async def run_deps(config: AppConfig, dependencies) -> AsyncGenerator[None, None]:
    to_close = []
    original_ex = None
    try:
        with _log_and_reraise():
            for enter_fn, exit_fn in dependencies:
                if enter_fn is not None:
                    if inspect.iscoroutinefunction(enter_fn):
                        await enter_fn(config)
                    else:
                        enter_fn(config)
                to_close.append(exit_fn)
        yield
    except Exception as e:  # pylint: disable=broad-exception-caught
        original_ex = e
    finally:
        with _log_and_reraise():
            to_raise = []
            if original_ex is not None:
                to_raise.append(original_ex)
            for exit_fn in to_close[::-1]:
                if exit_fn is None:
                    continue
                try:
                    exc_info = sys.exc_info()
                    if inspect.iscoroutinefunction(exit_fn):
                        await exit_fn(*exc_info)
                    else:
                        exit_fn(*exc_info)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    to_raise.append(e)
            if to_raise:
                raise RuntimeError(to_raise)
