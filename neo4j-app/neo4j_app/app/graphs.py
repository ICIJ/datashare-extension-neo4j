import logging

import neo4j
from fastapi import APIRouter, Depends
from starlette.responses import StreamingResponse

from neo4j_app.app.dependencies import neo4j_driver_dep
from neo4j_app.app.doc import (
    DOC_GRAPH_DUMP,
    DOC_GRAPH_DUMP_DESC,
    DOC_GRAPH_SCHEMA,
    GRAPH_TAG,
)
from neo4j_app.core.neo4j.dumps import dump_graph
from neo4j_app.core.neo4j.schema import graph_schema
from neo4j_app.core.objects import DumpRequest, GraphSchema
from neo4j_app.core.utils.logging import log_elapsed_time_cm

logger = logging.getLogger(__name__)


def graphs_router() -> APIRouter:
    router = APIRouter(prefix="/graphs", tags=[GRAPH_TAG])

    @router.post(
        "/dump",
        response_model=bytes,
        summary=DOC_GRAPH_DUMP,
        description=DOC_GRAPH_DUMP_DESC,
    )
    async def _graph_dump(
        database: str,
        payload: DumpRequest,
        neo4j_driver: neo4j.AsyncDriver = Depends(neo4j_driver_dep),
    ) -> StreamingResponse:
        with log_elapsed_time_cm(
            logger, logging.INFO, "Dumped graph in {elapsed_time} !"
        ):
            res = StreamingResponse(
                dump_graph(
                    dump_format=payload.format,
                    neo4j_driver=neo4j_driver,
                    neo4j_db=database,
                    query=payload.query,
                )
            )
        return res

    @router.get("/schema", response_model=GraphSchema, summary=DOC_GRAPH_SCHEMA)
    async def _graph_schema(
        database: str, neo4j_driver: neo4j.AsyncDriver = Depends(neo4j_driver_dep)
    ) -> GraphSchema:
        with log_elapsed_time_cm(
            logger, logging.INFO, "Return graph schema in {elapsed_time} !"
        ):
            schema = await graph_schema(neo4j_driver, neo4j_db=database)
        return schema

    return router
