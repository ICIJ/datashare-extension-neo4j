import logging
from typing import Optional

from neo4j import AsyncGraphDatabase

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.elasticsearch.documents import DEFAULT_SIZE, search_documents
from neo4j_app.core.elasticsearch.utils import HITS, TOTAL
from neo4j_app.core.objects import DocumentImportResponse
from neo4j_app.core.utils.asyncio import run_concurrently

logger = logging.getLogger(__name__)


async def import_documents(
        es: ESClient,
        neo4j: AsyncGraphDatabase,
        query: Optional[str] = None,
        *,
        neo4j_batch_size: int,
        scroll_size: int = DEFAULT_SIZE,
        max_concurrent_imports: int = 10,
) -> DocumentImportResponse:
    search = await search_documents(es, query, track_total_hits=True, size=scroll_size)

    n_to_insert = search[HITS][TOTAL]
    n_inserted = 0

    logger.debug("Starting insertion of %s documents into neo4j...", n_to_insert)
    async for partial_res in run_concurrently(
            neo4j async_batches(es.scroll_through(search), neo4j_batch_size),
    n_imports=max_concurrent_imports,
    ):
        n_inserted += partial_res.counters.nodes_created
        logger.debug("Imported (%s / %s)", n_inserted, n_to_insert)

    return DocumentImportResponse(n_to_insert=n_to_insert, n_inserted=n_inserted)
