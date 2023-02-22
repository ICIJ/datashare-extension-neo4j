from pathlib import Path

from fastapi import APIRouter, Depends, Request

from neo4j_app.app.dependencies import es_client_dep, neo4j_session_dep
from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.named_entities import import_named_entities
from neo4j_app.core.objects import IncrementalImportRequest, IncrementalImportResponse

NE_TAG = "Named entities"
_NE_IMPORT_SUM = "Import named entities from elasticsearch to neo4j"
_NE_IMPORT_DESC = """Named entities are searched for in `elasticsearch` potentially \
using the provided query they are then upserted into the `neo4j` database.

**Only named entities which document has been priorly imported into neo4j are imported**

They query must be an content of the `query` field of an elasticsearch query. When \
provided it will combined into `bool` query in order to restrict the provided query to \
 apply to named entities. 

If you provide:
```json
{
    "ids": {
        "values": ["someNamedEntityId"]
    }
}
```
the service will first look into neo4j for existing ID `["docId1",...,"docIdN"]` \
and then the query which will actually be performed will be:
```json
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "type": "NamedEntity"
                    }
                },
                {
                    "has_parent": {
                        "parent_type": "Document",
                        "query": {
                            "ids": {
                                "values": ["docId1",...,"docIdN"]
                            }
                        }
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


def named_entities_router() -> APIRouter:
    router = APIRouter(dependencies=[Depends(neo4j_session_dep)], tags=[NE_TAG])

    @router.post(
        "/named-entities",
        response_model=IncrementalImportResponse,
        summary=_NE_IMPORT_SUM,
        description=_NE_IMPORT_DESC,
    )
    async def _import_named_entities(
        payload: IncrementalImportRequest,
        request: Request,
        es_client: ESClient = Depends(es_client_dep),
    ) -> IncrementalImportResponse:
        neo4j_sess = request.state.neo4j_session
        config: AppConfig = request.app.state.config
        return await import_named_entities(
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
