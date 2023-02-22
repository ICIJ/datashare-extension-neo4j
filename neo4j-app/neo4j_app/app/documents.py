from pathlib import Path

from fastapi import APIRouter, Depends, Request

from neo4j_app.app.dependencies import es_client_dep, neo4j_session_dep
from neo4j_app.core import AppConfig
from neo4j_app.core.imports import import_documents
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.objects import IncrementalImportRequest, IncrementalImportResponse

DOCUMENT_TAG = "Documents"
_DOC_IMPORT_SUM = "Import documents from elasticsearch to neo4j"
_DOC_IMPORT_DESC = """Documents are searched for in `elasticsearch` potentially using \
the provided query they are then upserted into the `neo4j` database.

They query must be an content of the `query` field of an elasticsearch query. When \
provided it will combined into `bool` query in order to restrict the provided query to \
 apply to documents. 

If you provide:
```json
{
    "match": {
        "path": "somePath"
    }
}
```
then the query which will actually be performed will be:
```json
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "<docFieldType>": "Document"
                    }
                }
                {
                    "match": {
                        "path": "somePath"
                    }
                }           
            ]
        }
    }
}
```

The `<docFieldType>` defaults to `type` and is supposed to be forwarded from the Java \
app to the Python one through configuration. 
"""


def documents_router() -> APIRouter:
    router = APIRouter(dependencies=[Depends(neo4j_session_dep)], tags=[DOCUMENT_TAG])

    @router.post(
        "/documents",
        response_model=IncrementalImportResponse,
        summary=_DOC_IMPORT_SUM,
        description=_DOC_IMPORT_DESC,
    )
    async def _import_documents(
        payload: IncrementalImportRequest,
        request: Request,
        es_client: ESClient = Depends(es_client_dep),
    ) -> IncrementalImportResponse:
        neo4j_sess = request.state.neo4j_session
        config: AppConfig = request.app.state.config
        return await import_documents(
            query=payload.query,
            neo4j_session=neo4j_sess,
            es_client=es_client,
            neo4j_import_dir=Path(config.neo4j_import_dir),
            neo4j_import_prefix=config.neo4j_import_prefix,
            # TODO: take this one from the payload
            keep_alive=config.es_keep_alive,
            doc_type_field=config.es_doc_type_field,
            # TODO: take this one from the payload
            concurrency=es_client.max_concurrency,
        )

    return router
