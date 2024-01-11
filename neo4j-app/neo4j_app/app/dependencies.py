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
from typing import AsyncGenerator, List, Optional, cast

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
from neo4j_app.icij_worker.task_manager import TaskManager
from neo4j_app.icij_worker.task_manager.neo4j import Neo4JTaskManager

logger = logging.getLogger(__name__)

_CONFIG: Optional[AppConfig] = None
_ES_CLIENT: Optional[ESClientABC] = None
_EVENT_PUBLISHER: Optional[EventPublisher] = None
_PROCESS_MANAGER: Optional[SyncManager] = None
_NEO4J_DRIVER: Optional[neo4j.AsyncDriver] = None
_TASK_MANAGER: Optional[TaskManager] = None
_TEST_DB_FILE: Optional[Path] = None
_TEST_LOCK: Optional[multiprocessing.Lock] = None
_WORKER_POOL: Optional[multiprocessing.Pool] = None
_MP_CONTEXT = None


class DependencyInjectionError(RuntimeError):
    def __init__(self, name: str):
        msg = f"{name} was not injected"
        super().__init__(msg)


def config_enter(config: AppConfig, **_):
    global _CONFIG
    _CONFIG = config
    logger.info("Loaded config %s", config.json(indent=2))


def loggers_enter(**_):
    config = lifespan_config()
    config.setup_loggers()
    logger.info("Loggers ready to log 💬")


def lifespan_config() -> AppConfig:
    if _CONFIG is None:
        raise DependencyInjectionError("config")
    return cast(AppConfig, _CONFIG)


def mp_context_enter(**__):
    global _MP_CONTEXT
    _MP_CONTEXT = multiprocessing.get_context("spawn")


def lifespan_mp_context():
    if _MP_CONTEXT is None:
        raise DependencyInjectionError("multiprocessing context")
    return _MP_CONTEXT


async def neo4j_driver_enter(**__):
    global _NEO4J_DRIVER
    _NEO4J_DRIVER = lifespan_config().to_neo4j_driver()
    await _NEO4J_DRIVER.__aenter__()  # pylint: disable=unnecessary-dunder-call

    logger.debug("pinging neo4j...")
    async with _NEO4J_DRIVER.session(database=neo4j.SYSTEM_DATABASE) as sess:
        await sess.run("CALL db.ping()")
    logger.debug("neo4j driver is ready")


async def neo4j_driver_exit(exc_type, exc_value, trace):
    already_closed = False
    try:
        await _NEO4J_DRIVER.verify_connectivity()
    except:  # pylint: disable=bare-except
        already_closed = True
    if not already_closed:
        await _NEO4J_DRIVER.__aexit__(exc_type, exc_value, trace)


def lifespan_neo4j_driver() -> neo4j.AsyncDriver:
    if _NEO4J_DRIVER is None:
        raise DependencyInjectionError("neo4j driver")
    return cast(neo4j.AsyncDriver, _NEO4J_DRIVER)


async def es_client_enter(**_):
    global _ES_CLIENT
    _ES_CLIENT = lifespan_config().to_es_client()
    await _ES_CLIENT.__aenter__()  # pylint: disable=unnecessary-dunder-call


async def es_client_exit(exc_type, exc_value, trace):
    await _ES_CLIENT.__aexit__(exc_type, exc_value, trace)


def lifespan_es_client() -> ESClientABC:
    if _ES_CLIENT is None:
        raise DependencyInjectionError("es client")
    return cast(ESClientABC, _ES_CLIENT)


def test_db_path_enter(**_):
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
    config = lifespan_config()
    if config.test:
        global _TEST_LOCK
        _TEST_LOCK = lifespan_test_process_manager().Lock()


def _lifespan_test_lock() -> multiprocessing.Lock:
    if _TEST_LOCK is None:
        raise DependencyInjectionError("test lock")
    return cast(multiprocessing.Lock, _TEST_LOCK)


def worker_pool_enter(**_):
    # pylint: disable=consider-using-with
    global _WORKER_POOL
    process_id = os.getpid()
    config = lifespan_config()
    n_workers = min(config.neo4j_app_n_async_workers, config.neo4j_app_task_queue_size)
    n_workers = max(1, n_workers)
    # TODO: let the process choose they ID and set it with the worker process ID,
    #  this will help debugging
    worker_ids = [f"worker-{process_id}-{i}" for i in range(n_workers)]
    _WORKER_POOL = lifespan_mp_context().Pool(
        processes=config.neo4j_app_n_async_workers, maxtasksperchild=1
    )
    _WORKER_POOL.__enter__()  # pylint: disable=unnecessary-dunder-call
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
    pool = lifespan_worker_pool()
    pool.__exit__(exc_type, exc_value, trace)
    logger.debug("async worker pool has shut down !")


def lifespan_worker_pool() -> multiprocessing.Pool:
    if _WORKER_POOL is None:
        raise DependencyInjectionError("worker pool")
    return cast(multiprocessing.Pool, _WORKER_POOL)


def task_manager_enter(**_):
    global _TASK_MANAGER
    config = lifespan_config()
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
    config = lifespan_config()
    if config.test:
        from neo4j_app.tests.icij_worker.conftest import MockEventPublisher

        _EVENT_PUBLISHER = MockEventPublisher(
            _lifespan_test_db_path(), _lifespan_test_lock()
        )
    else:
        _EVENT_PUBLISHER = Neo4jEventPublisher(lifespan_neo4j_driver())


async def create_project_registry_db_enter(**_):
    driver = lifespan_neo4j_driver()
    await create_project_registry_db(driver)


async def migrate_app_db_enter(config: AppConfig):
    logger.info("Running schema migrations...")
    driver = lifespan_neo4j_driver()
    if config.force_migrations:
        # TODO: improve this as is could lead to race conditions...
        logger.info("Deleting all previous migrations...")
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
    ("configuration reading", config_enter, None),
    ("loggers setup", loggers_enter, None),
    ("neo4j driver creation", neo4j_driver_enter, neo4j_driver_exit),
    ("neo4j project registry creation", create_project_registry_db_enter, None),
    ("ES client creation", es_client_enter, es_client_exit),
    (None, mp_context_enter, None),
    (None, test_process_manager_enter, test_process_manager_exit),
    (None, test_db_path_enter, test_db_path_exit),
    (None, _test_lock_enter, None),
    ("task manager creation", task_manager_enter, None),
    ("event publisher creation", event_publisher_enter, None),
    ("async worker pool creation", worker_pool_enter, worker_pool_exit),
    ("neo4j DB migration", migrate_app_db_enter, None),
]


@contextmanager
def _log_exceptions():
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
async def run_app_deps(app: FastAPI, dependencies: List) -> AsyncGenerator[None, None]:
    async with run_deps(dependencies, config=app.state.config):
        yield


@asynccontextmanager
async def run_deps(dependencies: List, **kwargs) -> AsyncGenerator[None, None]:
    to_close = []
    original_ex = None
    try:
        with _log_exceptions():
            logger.info("applying dependencies...")
            for name, enter_fn, exit_fn in dependencies:
                if enter_fn is not None:
                    if name is not None:
                        logger.debug("applying: %s", name)
                    if inspect.iscoroutinefunction(enter_fn):
                        await enter_fn(**kwargs)
                    else:
                        enter_fn(**kwargs)
                to_close.append((name, exit_fn))
        yield
    except Exception as e:  # pylint: disable=broad-exception-caught
        original_ex = e
    finally:
        to_raise = []
        if original_ex is not None:
            to_raise.append(original_ex)
        logger.info("rolling back dependencies...")
        for name, exit_fn in to_close[::-1]:
            if exit_fn is None:
                continue
            try:
                if name is not None:
                    logger.debug("rolling back %s", name)
                exc_info = sys.exc_info()
                with _log_exceptions():
                    if inspect.iscoroutinefunction(exit_fn):
                        await exit_fn(*exc_info)
                    else:
                        exit_fn(*exc_info)
            except Exception as e:  # pylint: disable=broad-exception-caught
                to_raise.append(e)
            logger.debug("rolled back all dependencies !")
            if to_raise:
                raise RuntimeError(to_raise)
