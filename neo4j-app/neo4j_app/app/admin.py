import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Request

from neo4j_app.app.dependencies import (
    es_client_dep,
)
from neo4j_app.app.doc import (
    ADMIN_TAG,
    DOC_NEO4J_CSV,
    DOC_NEO4J_CSV_DESC,
)
from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.imports import to_neo4j_csvs
from neo4j_app.core.objects import (
    Neo4jCSVRequest,
    Neo4jCSVResponse,
)
from neo4j_app.core.utils.logging import log_elapsed_time_cm

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
        database: str,
        payload: Neo4jCSVRequest,
        request: Request,
        es_client: ESClientABC = Depends(es_client_dep),
    ) -> Neo4jCSVResponse:
        config: AppConfig = request.app.state.config

        with log_elapsed_time_cm(
            logger, logging.INFO, "Exported ES to CSV in {elapsed_time} !"
        ):
            res = await to_neo4j_csvs(
                export_dir=Path(payload.export_dir),
                es_query=payload.query,
                es_client=es_client,
                es_concurrency=config.es_max_concurrency,
                es_keep_alive=config.es_keep_alive,
                es_doc_type_field=config.es_doc_type_field,
                neo4j_db=database,
            )
        return res

    return router
