import logging

from fastapi import APIRouter, Request
from starlette.responses import StreamingResponse

from neo4j_app.app import ServiceConfig
from neo4j_app.app.dependencies import lifespan_neo4j_driver
from neo4j_app.app.doc import DOC_GRAPH_DUMP, DOC_GRAPH_DUMP_DESC, GRAPH_TAG
from neo4j_app.core.neo4j.graphs import dump_graph, project_statistics
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
        request: Request,
    ) -> StreamingResponse:
        config: ServiceConfig = request.app.state.config
        if config.supports_neo4j_parallel_runtime is None:
            msg = (
                "parallel support has not been set, config has not been properly"
                " initialized using AppConfig.with_neo4j_support"
            )
            raise ValueError(msg)
        parallel = False  # Parallel seem to slow down export let's deactivate it
        res = StreamingResponse(
            dump_graph(
                project=project,
                dump_format=payload.format,
                neo4j_driver=lifespan_neo4j_driver(),
                query=payload.query,
                default_docs_limit=config.neo4j_app_max_dumped_documents,
                parallel=parallel,
                export_batch_size=config.neo4j_export_batch_size,
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
            stats = await project_statistics(
                project=project, neo4j_driver=lifespan_neo4j_driver()
            )
        return stats.counts

    return router
