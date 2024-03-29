import logging
import os
from pathlib import Path
from tempfile import mkdtemp

from fastapi import APIRouter, Request
from icij_common.logging_utils import log_elapsed_time_cm

from neo4j_app.app import ServiceConfig
from neo4j_app.app.dependencies import (
    lifespan_neo4j_driver,
)
from neo4j_app.app.doc import (
    ADMIN_TAG,
    DOC_NEO4J_CSV,
    DOC_NEO4J_CSV_DESC,
)
from neo4j_app.core.imports import to_neo4j_csvs
from neo4j_app.core.objects import (
    Neo4jCSVRequest,
    Neo4jCSVResponse,
)
from neo4j_app.tasks.dependencies import lifespan_es_client

logger = logging.getLogger(__name__)


def admin_router() -> APIRouter:
    router = APIRouter(prefix="/admin", tags=[ADMIN_TAG])

    @router.post(
        "/neo4j-csvs",
        response_model=Neo4jCSVResponse,
        summary=DOC_NEO4J_CSV,
        description=DOC_NEO4J_CSV_DESC,
    )
    async def _neo4j_csv(
        project: str, payload: Neo4jCSVRequest, request: Request
    ) -> Neo4jCSVResponse:
        config: ServiceConfig = request.app.state.config

        with log_elapsed_time_cm(
            logger, logging.INFO, "Exported ES to CSV in {elapsed_time} !"
        ):
            tmpdir = mkdtemp()
            res = await to_neo4j_csvs(
                project=project,
                export_dir=Path(tmpdir),
                es_query=payload.query,
                es_client=lifespan_es_client(),
                es_concurrency=config.es_max_concurrency,
                es_keep_alive=config.es_keep_alive,
                es_doc_type_field=config.es_doc_type_field,
                neo4j_driver=lifespan_neo4j_driver(),
            )
        os.chmod(tmpdir, 0o744)
        os.chmod(res.path, 0o744)
        return res

    return router
