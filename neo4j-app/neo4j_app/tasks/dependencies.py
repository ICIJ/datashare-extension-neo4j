import logging
from pathlib import Path
from typing import Optional, cast

import neo4j
from icij_common.neo4j.migrate import delete_all_migrations, migrate_db_schemas
from icij_common.neo4j.projects import create_project_registry_db
from icij_worker.utils.dependencies import DependencyInjectionError

from neo4j_app.app import ServiceConfig
from neo4j_app.config import AppConfig
from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.neo4j import MIGRATIONS

logger = logging.getLogger(__name__)

_CONFIG: Optional[ServiceConfig] = None
_ASYNC_APP_CONFIG: Optional[AppConfig] = None
_ES_CLIENT: Optional[ESClientABC] = None
_ASYNC_APP_CONFIG_PATH: Optional[Path] = None
_NEO4J_DRIVER: Optional[neo4j.AsyncDriver] = None


def config_enter(config: AppConfig, **_):
    global _CONFIG
    _CONFIG = config
    logger.info("Loaded config %s", config.json(indent=2))


async def config_from_path_enter(config_path: Path, **_):
    global _CONFIG
    with config_path.open() as f:
        config = AppConfig.from_java_properties(f)
    config = await config.with_neo4j_support()
    _CONFIG = config
    logger.info("Loaded config %s", config.json(indent=2))


async def config_neo4j_support_enter(**_):
    global _CONFIG
    config = lifespan_config()
    _CONFIG = await config.with_neo4j_support()


def lifespan_config() -> ServiceConfig:
    if _CONFIG is None:
        raise DependencyInjectionError("config")
    return _CONFIG


def loggers_enter(worker_id: str, **_):
    config = lifespan_config()
    config.setup_loggers(worker_id=worker_id)
    logger.info("worker loggers ready to log 💬")


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


async def create_project_registry_db_enter(**_):
    driver = lifespan_neo4j_driver()
    await create_project_registry_db(driver)


async def migrate_app_db_enter(**_):
    logger.info("Running schema migrations...")
    config = lifespan_config()
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


ASYNC_APP_LIFESPAN_DEPS = [
    ("configuration loading", config_from_path_enter, None),
    ("loggers setup", loggers_enter, None),
    ("neo4j driver creation", neo4j_driver_enter, neo4j_driver_exit),
    # This has to be done after the neo4j driver creation, once we know we can reach
    # the neo4j server
    ("add configuration neo4j support", config_neo4j_support_enter, None),
    ("neo4j project registry creation", create_project_registry_db_enter, None),
    ("neo4j DB migration", migrate_app_db_enter, None),
    ("ES client creation", es_client_enter, es_client_exit),
]
