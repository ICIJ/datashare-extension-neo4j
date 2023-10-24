import logging

from fastapi import APIRouter
from starlette.responses import StreamingResponse

from neo4j_app.app.dependencies import lifespan_neo4j_driver
from neo4j_app.app.doc import DOC_GRAPH_DUMP, DOC_GRAPH_DUMP_DESC, GRAPH_TAG
from neo4j_app.core.neo4j.graphs import count_documents_and_named_entities, dump_graph
from neo4j_app.core.objects import DumpRequest, GraphCounts
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
        project: str,
        payload: DumpRequest,
    ) -> StreamingResponse:
        with log_elapsed_time_cm(
            logger, logging.INFO, "Dumped graph in {elapsed_time} !"
        ):
            res = StreamingResponse(
                dump_graph(
                    project=project,
                    dump_format=payload.format,
                    neo4j_driver=lifespan_neo4j_driver(),
                    query=payload.query,
                ),
                media_type="binary/octet-stream",
            )
        return res

    @router.get("/counts", response_model=GraphCounts)
    async def _count_documents_and_named_entities(project: str) -> GraphCounts:
        with log_elapsed_time_cm(
            logger,
            logging.INFO,
            "Counted documents and named entities in {elapsed_time} !",
        ):
            count = await count_documents_and_named_entities(
                project=project, neo4j_driver=lifespan_neo4j_driver()
            )
        return count

    return router
