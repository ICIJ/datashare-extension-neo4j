import logging
import os
from pathlib import Path
from typing import Dict, Optional

import neo4j

from neo4j_app.constants import NE_COLUMNS
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.elasticsearch.to_neo4j import es_to_neo4j_named_entity
from neo4j_app.core.elasticsearch.utils import (
    ES_DOCUMENT_TYPE,
    ES_NAMED_ENTITY_TYPE,
    and_query,
    has_id,
    has_parent,
    has_type,
)
from neo4j_app.core.neo4j.documents import (
    documents_ids_tx,
)
from neo4j_app.core.neo4j import get_neo4j_csv_writer, make_neo4j_import_file
from neo4j_app.core.neo4j.named_entities import import_named_entities_from_csv_tx
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.core.utils.logging import log_elapsed_time, log_elapsed_time_cm

logger = logging.getLogger(__name__)


@log_elapsed_time(logger, logging.INFO, "Imported named entities in {elapsed_time} !")
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

    with make_neo4j_import_file(
        neo4j_import_dir=neo4j_import_dir, neo4j_import_prefix=neo4j_import_prefix
    ) as (f, neo4j_import_path):
        # Make import file accessible to neo4j
        os.chmod(f.name, 0o777)
        header = sorted(NE_COLUMNS)
        writer = get_neo4j_csv_writer(f, header=header)
        writer.writeheader()
        f.flush()
        n_to_insert = await es_client.write_concurrently_neo4j_csv(
            query,
            f,
            header=header,
            keep_alive=keep_alive,
            concurrency=concurrency,
            es_to_neo4j=es_to_neo4j_named_entity,
        )
        with log_elapsed_time_cm(
            logger,
            logging.DEBUG,
            "Imported named entities from csv to neo4j in {elapsed_time}",
        ):
            summary: neo4j.ResultSummary = await neo4j_session.execute_write(
                import_named_entities_from_csv_tx, neo4j_import_path=neo4j_import_path
            )
    n_inserted = summary.counters.nodes_created
    response = IncrementalImportResponse(n_to_insert=n_to_insert, n_inserted=n_inserted)
    return response
