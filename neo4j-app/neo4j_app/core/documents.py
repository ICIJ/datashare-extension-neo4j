from pathlib import Path
from typing import Dict, Optional

import neo4j

from neo4j_app.constants import DOC_COLUMNS
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.elasticsearch.documents import to_document_csv
from neo4j_app.core.elasticsearch.utils import ES_DOCUMENT_TYPE, QUERY, TERM, and_query
from neo4j_app.core.neo4j.documents import (
    import_documents_from_csv_tx,
    make_neo4j_import_file,
    write_neo4j_csv,
)
from neo4j_app.core.objects import DocumentImportResponse


async def import_documents(
    *,
    neo4j_session: neo4j.AsyncSession,
    es_client: ESClient,
    neo4j_import_dir: Path,
    query: Optional[Dict],
    scroll: str,
    scroll_size: int,
    doc_type_field: str,
) -> DocumentImportResponse:
    # Let's restrict the search to documents, the type is a keyword property
    # we can safely use a term query
    document_type_query = {TERM: {doc_type_field: ES_DOCUMENT_TYPE}}
    if query is not None and query:
        query = and_query(document_type_query, query)
    else:
        query = {QUERY: document_type_query}
    # pylint: disable=line-too-long
    # TODO: stream concurrently from ES to the CSV using either:
    #  - PIT + slice: https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
    #  - a slice scroll: https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#slice-scroll
    # pylint: enable=line-too-long
    docs = (
        d
        async for d in es_client.async_scan(
            query=query, scroll=scroll, scroll_size=scroll_size
        )
    )
    with make_neo4j_import_file(neo4j_import_dir) as (f, neo4j_import_path):
        n_docs_to_insert = await write_neo4j_csv(
            f, rows=to_document_csv(docs), header=sorted(DOC_COLUMNS)
        )
        f.flush()
        # Here we might need to use a autocommit transaction in case we use periodic
        # commits ?
        summary: neo4j.ResultSummary = await neo4j_session.execute_write(
            import_documents_from_csv_tx, neo4j_import_path=neo4j_import_path
        )
    n_inserted_docs = summary.counters.nodes_created
    response = DocumentImportResponse(
        n_docs_to_insert=n_docs_to_insert, n_inserted_docs=n_inserted_docs
    )
    return response
