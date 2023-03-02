import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol

import neo4j

from neo4j_app.constants import DOC_COLUMNS, NE_COLUMNS
from neo4j_app.core.elasticsearch import ESClient
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
from neo4j_app.core.neo4j import (
    get_neo4j_csv_writer,
    make_neo4j_import_file,
)
from neo4j_app.core.neo4j.documents import (
    documents_ids_tx,
    import_documents_from_csv_tx,
)
from neo4j_app.core.neo4j.named_entities import import_named_entities_from_csv_tx
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.core.utils.logging import log_elapsed_time_cm

logger = logging.getLogger(__name__)


class ImportTransactionFunction(Protocol):
    async def __call__(
        self, tx: neo4j.AsyncTransaction, neo4j_import_path: Path, **kwargs
    ) -> Any:
        ...


async def import_documents(
    *,
    query: Optional[Dict],
    neo4j_session: neo4j.AsyncSession,
    es_client: ESClient,
    neo4j_import_dir: Path,
    neo4j_import_prefix: Optional[str] = None,
    keep_alive: Optional[str] = None,
    doc_type_field: str,
    concurrency: Optional[int] = None,
) -> IncrementalImportResponse:
    # Let's restrict the search to documents, the type is a keyword property
    # we can safely use a term query
    document_type_query = has_type(
        type_field=doc_type_field, type_value=ES_DOCUMENT_TYPE
    )
    if query is not None and query:
        query = and_query(document_type_query, query)
    else:
        query = {QUERY: document_type_query}
    response = await _es_to_neo4j_import(
        query=query,
        header=DOC_COLUMNS,
        import_tx=import_documents_from_csv_tx,
        es_to_neo4j=es_to_neo4j_doc,
        neo4j_session=neo4j_session,
        es_client=es_client,
        neo4j_import_dir=neo4j_import_dir,
        neo4j_import_prefix=neo4j_import_prefix,
        keep_alive=keep_alive,
        concurrency=concurrency,
        imported_entity_label="documents",
    )
    return response


async def import_named_entities(
    *,
    query: Optional[Dict],
    neo4j_session: neo4j.AsyncSession,
    es_client: ESClient,
    neo4j_import_dir: Path,
    neo4j_import_prefix: Optional[str] = None,
    keep_alive: Optional[str] = None,
    doc_type_field: str,
    concurrency: Optional[int] = None,
) -> IncrementalImportResponse:
    if concurrency is None:
        concurrency = es_client.max_concurrency
    document_ids = await neo4j_session.execute_read(documents_ids_tx)
    # Since this is an incremental import we consider it reasonable to use an ES join,
    # however for named entities bulk import join should be avoided and post filtering
    # on the documentId will probably be much more efficient !
    # TODO: if joining is too slow, switch to post filtering
    queries = [
        has_type(type_field=doc_type_field, type_value=ES_NAMED_ENTITY_TYPE),
        has_parent(parent_type=ES_DOCUMENT_TYPE, query=has_id(document_ids)),
    ]
    if query is not None and query:
        queries.append(query)
    query = and_query(*queries)
    response = await _es_to_neo4j_import(
        query=query,
        header=NE_COLUMNS,
        import_tx=import_named_entities_from_csv_tx,
        es_to_neo4j=es_to_neo4j_named_entity,
        neo4j_session=neo4j_session,
        es_client=es_client,
        neo4j_import_dir=neo4j_import_dir,
        neo4j_import_prefix=neo4j_import_prefix,
        keep_alive=keep_alive,
        concurrency=concurrency,
        imported_entity_label="named entities",
    )
    return response


async def _es_to_neo4j_import(
    *,
    query: Optional[Dict],
    header: List[str],
    import_tx: ImportTransactionFunction,
    es_to_neo4j: Callable[[Dict[str, Any]], Dict[str, str]],
    neo4j_session: neo4j.AsyncSession,
    es_client: ESClient,
    neo4j_import_dir: Path,
    neo4j_import_prefix: Optional[str] = None,
    keep_alive: Optional[str] = None,
    concurrency: Optional[int] = None,
    imported_entity_label: str,
) -> IncrementalImportResponse:
    with make_neo4j_import_file(
        neo4j_import_dir=neo4j_import_dir, neo4j_import_prefix=neo4j_import_prefix
    ) as (f, neo4j_import_path):
        writer = get_neo4j_csv_writer(f, header=header)
        writer.writeheader()
        f.flush()

        with log_elapsed_time_cm(
            logger, logging.DEBUG, "Exported ES query to neo4j csv in {elapsed_time} !"
        ):
            n_to_insert = await es_client.write_concurrently_neo4j_csv(
                query,
                f,
                header=header,
                keep_alive=keep_alive,
                concurrency=concurrency,
                es_to_neo4j=es_to_neo4j,
            )
        with log_elapsed_time_cm(
            logger,
            logging.DEBUG,
            f"Imported {imported_entity_label} from csv to neo4j in {{elapsed_time}}",
        ):
            summary: neo4j.ResultSummary = await neo4j_session.execute_write(
                import_tx, neo4j_import_path=neo4j_import_path
            )
    n_inserted = summary.counters.nodes_created
    response = IncrementalImportResponse(n_to_insert=n_to_insert, n_inserted=n_inserted)
    return response
