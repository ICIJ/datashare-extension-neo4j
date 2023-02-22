import logging
import os
from pathlib import Path
from typing import Dict, Optional

import neo4j

from neo4j_app.constants import DOC_COLUMNS
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.elasticsearch.to_neo4j import es_to_neo4j_doc
from neo4j_app.core.elasticsearch.utils import (
    ES_DOCUMENT_TYPE,
    QUERY,
    and_query,
    has_type,
)
from neo4j_app.core.neo4j import (
    make_neo4j_import_file,
    write_neo4j_csv,
)
from neo4j_app.core.neo4j.documents import (
    import_documents_from_csv_tx,
)
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.core.utils.logging import log_elapsed_time

logger = logging.getLogger(__name__)


@log_elapsed_time(logger, logging.INFO, "Imported documents in {elapsed_time} !")
async def import_documents(
    *,
    neo4j_session: neo4j.AsyncSession,
    es_client: ESClient,
    neo4j_import_dir: Path,
    neo4j_import_prefix: Optional[str] = None,
    query: Optional[Dict],
    scroll: str,
    scroll_size: int,
    doc_type_field: str,
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
    # pylint: disable=line-too-long
    # TODO: stream concurrently from ES to the CSV using either:
    #  - PIT + slice: https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
    #  - a slice scroll: https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#slice-scroll
    # pylint: enable=line-too-long
    docs = []
    async for d in es_client.async_scan(
        query=query, scroll=scroll, scroll_size=scroll_size
    ):
        docs.append(d)

    # TODO: update this to use concurrent write
    with make_neo4j_import_file(
        neo4j_import_dir=neo4j_import_dir, neo4j_import_prefix=neo4j_import_prefix
    ) as (f, neo4j_import_path):
        header = sorted(DOC_COLUMNS)
        docs = (es_to_neo4j_doc(doc) for doc in docs)
        n_docs_to_insert = write_neo4j_csv(
            f, rows=docs, header=header, write_header=True
        )
        f.flush()
        # # Make import file accessible to neo4j
        os.chmod(f.name, 0o777)
        # Here we might need to use a autocommit transaction in case we use periodic
        # commits ?
        summary: neo4j.ResultSummary = await neo4j_session.execute_write(
            import_documents_from_csv_tx, neo4j_import_path=neo4j_import_path
        )
    n_inserted_docs = summary.counters.nodes_created
    response = IncrementalImportResponse(
        n_to_insert=n_docs_to_insert, n_inserted=n_inserted_docs
    )
    return response
