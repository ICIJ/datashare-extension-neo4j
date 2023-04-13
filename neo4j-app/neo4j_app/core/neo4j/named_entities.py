from typing import Dict, List

import neo4j

from neo4j_app.constants import (
    DOC_ID,
    DOC_NODE,
    NE_APPEARS_IN_DOC,
    NE_CATEGORY,
    NE_DOC_ID,
    NE_EXTRACTOR,
    NE_EXTRACTORS,
    NE_EXTRACTOR_LANG,
    NE_ID,
    NE_IDS,
    NE_MENTION_NORM,
    NE_NODE,
    NE_OFFSETS,
)


async def import_named_entity_rows(
    neo4j_session: neo4j.AsyncSession,
    records: List[Dict],
    *,
    transaction_batch_size: int,
) -> neo4j.ResultSummary:
    # pylint: disable=line-too-long
    # TODO: see if we can avoid the apoc.coll.toSet
    query = f"""UNWIND $rows AS row
CALL {{
    WITH row
    CALL apoc.merge.node(["{NE_NODE}", row.{NE_CATEGORY}], {{{NE_MENTION_NORM}: row.{NE_MENTION_NORM}}}) YIELD node as mention
    MATCH (doc:{DOC_NODE} {{{DOC_ID}: row.{NE_DOC_ID}}})
    MERGE (mention)-[rel:{NE_APPEARS_IN_DOC}]->(doc)
    ON CREATE
        SET 
            rel.{NE_IDS} = [row.{NE_ID}],
            rel.{NE_EXTRACTORS} = [row.{NE_EXTRACTOR}],
            rel.{NE_EXTRACTOR_LANG} = row.{NE_EXTRACTOR_LANG},
            rel.{NE_OFFSETS} = row.{NE_OFFSETS}
    ON MATCH
        SET
            rel.{NE_IDS} = apoc.coll.toSet(rel.{NE_IDS} + row.{NE_ID}),
            rel.{NE_EXTRACTORS} = apoc.coll.toSet(rel.{NE_EXTRACTORS} + row.{NE_EXTRACTOR}),
            rel.{NE_OFFSETS} = apoc.coll.toSet(rel.{NE_OFFSETS} + row.{NE_OFFSETS})
}} IN TRANSACTIONS OF $batchSize ROWS
"""
    res = await neo4j_session.run(query, rows=records, batchSize=transaction_batch_size)
    summary = await res.consume()
    return summary
