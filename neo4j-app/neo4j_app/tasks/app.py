from neo4j_app.app.dependencies import (
    config_enter,
    es_client_enter,
    es_client_exit,
    loggers_enter,
    neo4j_driver_enter,
    neo4j_driver_exit,
)
from neo4j_app.icij_worker import ICIJApp

app = ICIJApp(name="neo4j-app")

WORKER_LIFESPAN_DEPS = [
    (config_enter, None),
    (loggers_enter, None),
    (neo4j_driver_enter, neo4j_driver_exit),
    (es_client_enter, es_client_exit),
]
