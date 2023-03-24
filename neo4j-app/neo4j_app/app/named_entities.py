import logging

import neo4j
from fastapi import APIRouter, Depends, Request

from neo4j_app.app.dependencies import (
    es_client_dep,
    neo4j_driver_dep,
)
from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.imports import import_named_entities
from neo4j_app.core.objects import IncrementalImportRequest, IncrementalImportResponse
from neo4j_app.core.utils.logging import log_elapsed_time_cm

logger = logging.getLogger(__name__)

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
    router = APIRouter(tags=[NE_TAG])

    @router.post(
        "/named-entities",
        response_model=IncrementalImportResponse,
        summary=_NE_IMPORT_SUM,
        description=_NE_IMPORT_DESC,
    )
    async def _import_named_entities(
        payload: IncrementalImportRequest,
        request: Request,
        neo4j_driver: neo4j.AsyncDriver = Depends(neo4j_driver_dep),
        es_client: ESClientABC = Depends(es_client_dep),
    ) -> IncrementalImportResponse:
        config: AppConfig = request.app.state.config
        with log_elapsed_time_cm(
            logger, logging.INFO, "Imported named entities in {elapsed_time} !"
        ):
            res = await import_named_entities(
                es_client=es_client,
                es_query=payload.query,
                es_concurrency=es_client.max_concurrency,
                es_keep_alive=config.es_keep_alive,
                es_doc_type_field=config.es_doc_type_field,
                neo4j_driver=neo4j_driver,
                neo4j_concurrency=config.neo4j_concurrency,
                neo4j_import_batch_size=config.neo4j_import_batch_size,
                neo4j_transaction_batch_size=config.neo4j_transaction_batch_size,
                max_records_in_memory=config.max_records_in_memory,
            )
        return res

    return router
