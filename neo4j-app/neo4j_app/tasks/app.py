import logging

from neo4j_app.icij_worker import AsyncApp
from neo4j_app.tasks.dependencies import WORKER_LIFESPAN_DEPS

logger = logging.getLogger(__name__)


app = AsyncApp(name="neo4j-app", dependencies=WORKER_LIFESPAN_DEPS)
