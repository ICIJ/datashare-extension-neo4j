import logging

import neo4j
from fastapi import APIRouter, Depends
from starlette.responses import Response

from neo4j_app.app.dependencies import get_global_config_dep, neo4j_driver_dep
from neo4j_app.app.doc import (
    DOC_PROJECT_INIT,
    PROJECT_TAG,
)
from neo4j_app.core import AppConfig
from neo4j_app.core.neo4j import MIGRATIONS
from neo4j_app.core.neo4j.migrations.migrate import init_project

logger = logging.getLogger(__name__)


def projects_router() -> APIRouter:
    router = APIRouter(prefix="/projects", tags=[PROJECT_TAG])

    @router.post(
        "/init",
        summary=DOC_PROJECT_INIT,
    )
    async def _init_project(
        project: str,
        config: AppConfig = Depends(get_global_config_dep),
        neo4j_driver: neo4j.AsyncDriver = Depends(neo4j_driver_dep),
    ):
        existed = await init_project(
            neo4j_driver=neo4j_driver,
            name=project,
            registry=MIGRATIONS,
            timeout_s=config.neo4j_app_migration_timeout_s,
            throttle_s=config.neo4j_app_migration_throttle_s,
        )
        status_code = 200 if existed else 201
        return Response(status_code=status_code)

    return router
