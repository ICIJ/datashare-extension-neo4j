import functools
import logging
from typing import Any, Callable, Dict, Optional, Protocol

import neo4j

from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.elasticsearch.to_neo4j import (
    es_to_neo4j_doc,
    es_to_neo4j_named_entity,
)
from neo4j_app.core.elasticsearch.utils import (
    ES_DOCUMENT_TYPE,
    ES_NAMED_ENTITY_TYPE,
    QUERY,
    and_query,
    has_id,
    has_parent,
    has_type,
)
from neo4j_app.core.neo4j import Neo4Import, Neo4jImportWorker
from neo4j_app.core.neo4j.documents import (
    documents_ids_tx,
    import_document_rows,
)
from neo4j_app.core.neo4j.named_entities import import_named_entity_rows
from neo4j_app.core.objects import IncrementalImportResponse

logger = logging.getLogger(__name__)


class ImportTransactionFunction(Protocol):
    async def __call__(
        self,
        neo4j_session: neo4j.AsyncSession,
        *,
        batch_size: int,
        **kwargs,
    ) -> neo4j.ResultSummary:
        ...


async def import_documents(
    *,
    es_client: ESClientABC,
    es_query: Optional[Dict],
    es_concurrency: Optional[int] = None,
    es_keep_alive: Optional[str] = None,
    es_doc_type_field: str,
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_concurrency: int,
    neo4j_import_batch_size: int,
    neo4j_transaction_batch_size: int,
    max_records_in_memory: int,
) -> IncrementalImportResponse:
    # Let's restrict the search to documents, the type is a keyword property
    # we can safely use a term query
    document_type_query = has_type(
        type_field=es_doc_type_field, type_value=ES_DOCUMENT_TYPE
    )
    if es_query is not None and es_query:
        es_query = and_query(document_type_query, es_query)
    else:
        es_query = {QUERY: document_type_query}
    response = await _es_to_neo4j_import(
        es_client=es_client,
        es_query=es_query,
        es_concurrency=es_concurrency,
        es_keep_alive=es_keep_alive,
        neo4j_driver=neo4j_driver,
        neo4j_import_fn=import_document_rows,
        neo4j_concurrency=neo4j_concurrency,
        neo4j_import_batch_size=neo4j_import_batch_size,
        neo4j_transaction_batch_size=neo4j_transaction_batch_size,
        to_neo4j_record=es_to_neo4j_doc,
        max_records_in_memory=max_records_in_memory,
        imported_entity_label="documents",
    )
    return response


async def import_named_entities(
    *,
    es_client: ESClientABC,
    es_query: Optional[Dict],
    es_concurrency: Optional[int] = None,
    es_keep_alive: Optional[str] = None,
    es_doc_type_field: str,
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_concurrency: int,
    neo4j_import_batch_size: int,
    neo4j_transaction_batch_size: int,
    max_records_in_memory: int,
) -> IncrementalImportResponse:
    async with neo4j_driver.session() as neo4j_session:
        document_ids = await neo4j_session.execute_read(documents_ids_tx)
    # Since this is an incremental import we consider it reasonable to use an ES join,
    # however for named entities bulk import join should be avoided and post filtering
    # on the documentId will probably be much more efficient !
    # TODO: if joining is too slow, switch to post filtering
    queries = [
        has_type(type_field=es_doc_type_field, type_value=ES_NAMED_ENTITY_TYPE),
        has_parent(parent_type=ES_DOCUMENT_TYPE, query=has_id(document_ids)),
    ]
    if es_query is not None and es_query:
        queries.append(es_query)
    es_query = and_query(*queries)
    response = await _es_to_neo4j_import(
        es_client=es_client,
        es_query=es_query,
        es_concurrency=es_concurrency,
        es_keep_alive=es_keep_alive,
        neo4j_driver=neo4j_driver,
        neo4j_import_fn=import_named_entity_rows,
        neo4j_concurrency=neo4j_concurrency,
        neo4j_import_batch_size=neo4j_import_batch_size,
        neo4j_transaction_batch_size=neo4j_transaction_batch_size,
        to_neo4j_record=es_to_neo4j_named_entity,
        max_records_in_memory=max_records_in_memory,
        imported_entity_label="named entities",
    )
    return response


def _make_neo4j_worker(
    name: str,
    neo4j_driver: neo4j.AsyncDriver,
    import_fn: Neo4Import,
    transaction_batch_size: int,
    to_neo4j_record: Callable[[Any], Dict],
) -> Neo4jImportWorker:
    return Neo4jImportWorker(
        name=name,
        neo4j_driver=neo4j_driver,
        import_fn=import_fn,
        transaction_batch_size=transaction_batch_size,
        to_neo4j_record=to_neo4j_record,
    )


async def _es_to_neo4j_import(
    *,
    es_client: ESClientABC,
    es_query: Optional[Dict],
    es_concurrency: Optional[int] = None,
    es_keep_alive: Optional[str] = None,
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_concurrency: int,
    neo4j_import_fn: Neo4Import,
    neo4j_import_batch_size: int,
    neo4j_transaction_batch_size: int,
    to_neo4j_record: Callable[[Any], Dict],
    max_records_in_memory: int,
    imported_entity_label: str,
) -> IncrementalImportResponse:
    neo4j_import_worker_factory = functools.partial(
        _make_neo4j_worker,
        neo4j_driver=neo4j_driver,
        import_fn=neo4j_import_fn,
        transaction_batch_size=neo4j_transaction_batch_size,
        to_neo4j_record=to_neo4j_record,
    )
    n_to_insert, summaries = await es_client.to_neo4j(
        es_query,
        neo4j_import_worker_factory=neo4j_import_worker_factory,
        num_neo4j_workers=neo4j_concurrency,
        import_batch_size=neo4j_import_batch_size,
        concurrency=es_concurrency,
        max_records_in_memory=max_records_in_memory,
        keep_alive=es_keep_alive,
        imported_entity_label=imported_entity_label,
    )
    n_inserted = sum(summary.counters.nodes_created for summary in summaries)
    response = IncrementalImportResponse(n_to_insert=n_to_insert, n_inserted=n_inserted)
    return response
