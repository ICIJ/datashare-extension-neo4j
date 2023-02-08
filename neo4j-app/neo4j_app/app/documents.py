from pathlib import Path

from fastapi import APIRouter, Depends, Request

from neo4j_app.app.dependencies import es_client_dep, neo4j_session_dep
from neo4j_app.core import AppConfig
from neo4j_app.core.documents import import_documents
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.objects import DocumentImportRequest, DocumentImportResponse

_DOCUMENT_TAG = "Documents"
_DOC_IMPORT_SUM = "Documents import from `elasticsearch` to `neo4j`"
_DOC_IMPORT_DESC = (
    "Documents are searched for in `elasticsearch` potentially using the"
    " provided query they are then upserted into the `neo4j` database"
)


def documents_router() -> APIRouter:
    router = APIRouter(dependencies=[Depends(neo4j_session_dep)])

    @router.post(
        "/documents",
        response_model=DocumentImportResponse,
        tags=[_DOCUMENT_TAG],
        summary=_DOC_IMPORT_SUM,
        description=_DOC_IMPORT_DESC,
    )
    async def _import_documents(
        payload: DocumentImportRequest,
        request: Request,
        es_client: ESClient = Depends(es_client_dep),
    ) -> DocumentImportResponse:
        neo4j_sess = request.state.neo4j_session
        config: AppConfig = request.app.state.config
        return await import_documents(
            query=payload.query,
            neo4j_session=neo4j_sess,
            es_client=es_client,
            neo4j_import_dir=Path(config.neo4j_import_dir),
            scroll=config.es_scroll,
            scroll_size=config.es_scroll_size,
            doc_type_field=config.es_doc_type_field,
        )

    return router
