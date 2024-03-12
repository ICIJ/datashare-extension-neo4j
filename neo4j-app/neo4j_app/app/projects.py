import logging

from fastapi import APIRouter
from icij_common.neo4j.migrate import init_project
from starlette.requests import Request
from starlette.responses import Response

from neo4j_app.app.dependencies import lifespan_neo4j_driver
from neo4j_app.app.doc import (
    DOC_PROJECT_INIT,
    PROJECT_TAG,
)
from neo4j_app.core.neo4j import MIGRATIONS

logger = logging.getLogger(__name__)


def projects_router() -> APIRouter:
    router = APIRouter(prefix="/projects", tags=[PROJECT_TAG])

    @router.post("/init", summary=DOC_PROJECT_INIT)
    async def _init_project(project: str, request: Request):
        config = request.app.state.config
        existed = await init_project(
            neo4j_driver=lifespan_neo4j_driver(),
            name=project,
            registry=MIGRATIONS,
            timeout_s=config.neo4j_app_migration_timeout_s,
            throttle_s=config.neo4j_app_migration_throttle_s,
        )
        status_code = 200 if existed else 201
        return Response(status_code=status_code)

    return router
