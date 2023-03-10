from pathlib import Path
from typing import List

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
)


async def import_documents_from_csv_tx(
    tx: neo4j.AsyncTransaction, neo4j_import_path: Path
) -> neo4j.ResultSummary:
    # TODO: use apoc.periodic.iterate(.., ..., {batchSize:10000, parallel:true,
    #  iterateList:true}) to || and save memory import...
    query = f"""LOAD CSV WITH HEADERS FROM 'file:///{neo4j_import_path}' AS row
MERGE (doc:{DOC_NODE} {{{DOC_ID}: row.{DOC_ID}}})
ON CREATE
    SET
        doc.{DOC_CONTENT_TYPE} = row.{DOC_CONTENT_TYPE},
        doc.{DOC_CONTENT_LENGTH} = toInteger(row.{DOC_CONTENT_LENGTH}),
        doc.{DOC_EXTRACTION_DATE} = datetime(row.{DOC_EXTRACTION_DATE}),
        doc.{DOC_DIRNAME} = row.{DOC_DIRNAME},
        doc.{DOC_PATH} = row.{DOC_PATH},
        doc.{DOC_ROOT_ID} = row.{DOC_ROOT_ID}
ON MATCH
    SET 
        doc.{DOC_CONTENT_TYPE} = row.{DOC_CONTENT_TYPE},
        doc.{DOC_CONTENT_LENGTH} = toInteger(row.{DOC_CONTENT_LENGTH}),
        doc.{DOC_EXTRACTION_DATE} = datetime(row.{DOC_EXTRACTION_DATE}),
        doc.{DOC_DIRNAME} = row.{DOC_DIRNAME},
        doc.{DOC_PATH} = row.{DOC_PATH},
        doc.{DOC_ROOT_ID} = row.{DOC_ROOT_ID}
"""
    res = await tx.run(query)
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
