import logging

from neo4j_app.app.dependencies import (
    config_enter,
    es_client_enter,
    es_client_exit,
    neo4j_driver_enter,
    neo4j_driver_exit,
)
from neo4j_app.core import AppConfig
from neo4j_app.icij_worker import ICIJApp

logger = logging.getLogger(__name__)
app = ICIJApp(name="neo4j-app")


def loggers_enter(config: AppConfig, worker_id: str):
    config.setup_loggers(worker_id=worker_id)
    logger.info("worker loggers ready to log 💬")


WORKER_LIFESPAN_DEPS = [
    (config_enter, None),
    (loggers_enter, None),
    (neo4j_driver_enter, neo4j_driver_exit),
    (es_client_enter, es_client_exit),
]
