import logging

from fastapi import APIRouter, Request

from neo4j_app.app.dependencies import lifespan_es_client, lifespan_neo4j_driver
from neo4j_app.app.doc import DOCUMENT_TAG, DOC_IMPORT_DESC, DOC_IMPORT_SUM
from neo4j_app.core import AppConfig
from neo4j_app.core.imports import import_documents
from neo4j_app.core.objects import IncrementalImportRequest, IncrementalImportResponse
from neo4j_app.core.utils.logging import log_elapsed_time_cm

logger = logging.getLogger(__name__)


def documents_router() -> APIRouter:
    router = APIRouter(tags=[DOCUMENT_TAG])

    @router.post(
        "/documents",
        response_model=IncrementalImportResponse,
        summary=DOC_IMPORT_SUM,
        description=DOC_IMPORT_DESC,
    )
    async def _import_documents(
        project: str,
        payload: IncrementalImportRequest,
        request: Request,
    ) -> IncrementalImportResponse:
        config: AppConfig = request.app.state.config
        with log_elapsed_time_cm(
            logger, logging.INFO, "Imported documents in {elapsed_time} !"
        ):
            es_client = lifespan_es_client()
            res = await import_documents(
                project=project,
                es_client=es_client,
                es_query=payload.query,
                es_concurrency=es_client.max_concurrency,
                es_keep_alive=config.es_keep_alive,
                es_doc_type_field=config.es_doc_type_field,
                neo4j_driver=lifespan_neo4j_driver(),
                neo4j_import_batch_size=config.neo4j_import_batch_size,
                neo4j_transaction_batch_size=config.neo4j_transaction_batch_size,
                max_records_in_memory=config.neo4j_app_max_records_in_memory,
            )
        return res

    return router
