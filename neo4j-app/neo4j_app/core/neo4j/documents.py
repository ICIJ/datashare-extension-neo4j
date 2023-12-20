import logging
from typing import Dict, List

import neo4j

from neo4j_app.constants import (
    DOC_CONTENT_LENGTH,
    DOC_CONTENT_TYPE,
    DOC_CREATED_AT,
    DOC_CREATED_AT_META,
    DOC_DIRNAME,
    DOC_EXTRACTION_DATE,
    DOC_ID,
    DOC_MODIFIED_AT,
    DOC_MODIFIED_AT_META,
    DOC_NODE,
    DOC_PATH,
    DOC_ROOT_ID,
    DOC_ROOT_TYPE,
    DOC_URL_SUFFIX,
)
from neo4j_app.typing_ import LightCounters

logger = logging.getLogger(__name__)


_DOC_CREATED_AT_META = ["metadata." + c for c in DOC_CREATED_AT_META]
_DOC_MODIFIED_AT_META = ["metadata." + c for c in DOC_MODIFIED_AT_META]


def _coalesce(*, variable: str, attributes: List[str]) -> str:
    values = ", ".join(f"{variable}.{a}" for a in attributes)
    return f"coalesce({values})"


async def import_document_rows(
    neo4j_session: neo4j.AsyncSession,
    records: List[Dict],
    *,
    transaction_batch_size: int,
) -> LightCounters:
    query = f"""UNWIND $rows AS row
WITH row
CALL {{
    WITH row    
    MERGE (doc:{DOC_NODE} {{{DOC_ID}: row.{DOC_ID}}})
    SET
        doc.{DOC_CONTENT_TYPE} = row.{DOC_CONTENT_TYPE},
        doc.{DOC_CONTENT_LENGTH} = toInteger(row.{DOC_CONTENT_LENGTH}),
        doc.{DOC_EXTRACTION_DATE} = datetime(row.{DOC_EXTRACTION_DATE}),
        doc.{DOC_DIRNAME} = row.{DOC_DIRNAME},
        doc.{DOC_PATH} = row.{DOC_PATH},
        doc.{DOC_URL_SUFFIX} = row.{DOC_URL_SUFFIX},
        doc.{DOC_CREATED_AT} = datetime({
    _coalesce(variable="row", attributes=_DOC_CREATED_AT_META)}), 
        doc.{DOC_MODIFIED_AT} = datetime({
    _coalesce(variable="row", attributes=_DOC_MODIFIED_AT_META)}) 
    WITH doc, row
    WHERE doc.{DOC_ID} = row.{DOC_ID} and row.{DOC_ROOT_ID} IS NOT NULL
    MERGE (root:{DOC_NODE} {{{DOC_ID}: row.{DOC_ROOT_ID}}})
    MERGE (doc)-[:{DOC_ROOT_TYPE}]->(root)
}} IN TRANSACTIONS OF $batchSize ROWS
"""
    res = await neo4j_session.run(query, rows=records, batchSize=transaction_batch_size)
    summary = await res.consume()
    counters = LightCounters(
        nodes_created=summary.counters.nodes_created,
        relationships_created=summary.counters.relationships_created,
    )
    return counters


async def documents_ids_tx(tx: neo4j.AsyncTransaction) -> List[str]:
    res = await tx.run(document_ids_query())
    doc_ids = await res.single()
    doc_ids = doc_ids.get("docIds")
    return doc_ids


def document_ids_query() -> str:
    # Collect on the neo4j side to gain time
    query = f"""MATCH (doc:{DOC_NODE})
RETURN collect(doc.{DOC_ID}) as docIds
"""
    return query
