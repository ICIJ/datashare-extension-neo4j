from typing import Dict, List

import neo4j

from neo4j_app.constants import (
    DOC_CONTENT_LENGTH,
    DOC_CONTENT_TYPE,
    DOC_DIRNAME,
    DOC_EXTRACTION_DATE,
    DOC_ID,
    DOC_NODE,
    DOC_PATH,
    DOC_ROOT_ID,
    DOC_ROOT_TYPE,
)


async def import_document_rows(
    neo4j_session: neo4j.AsyncSession,
    records: List[Dict],
    *,
    transaction_batch_size: int,
) -> neo4j.ResultSummary:
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
        doc.{DOC_PATH} = row.{DOC_PATH}
    WITH doc, row
    WHERE doc.{DOC_ID} = row.{DOC_ID} and row.{DOC_ROOT_ID} IS NOT NULL
    MERGE (root:{DOC_NODE} {{{DOC_ID}: row.{DOC_ROOT_ID}}})
    MERGE (doc)-[:{DOC_ROOT_TYPE}]->(root)
}} IN TRANSACTIONS OF $batchSize ROWS
"""
    res = await neo4j_session.run(query, rows=records, batchSize=transaction_batch_size)
    summary = await res.consume()
    return summary


async def documents_ids_tx(tx: neo4j.AsyncTransaction) -> List[str]:
    res = await tx.run(document_ids_query())
    res = [doc_id.value(DOC_ID) async for doc_id in res]
    return res


def document_ids_query() -> str:
    query = f"""MATCH (doc:{DOC_NODE})
RETURN doc.{DOC_ID} as {DOC_ID}
"""
    return query
