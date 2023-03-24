from typing import Dict, List

import neo4j

from neo4j_app.constants import (
    NE_CATEGORY,
    NE_DOC_ID,
    NE_EXTRACTOR,
    NE_EXTRACTOR_LANG,
    NE_ID,
    NE_MENTION,
    NE_MENTION_NORM,
    NE_MENTION_NORM_TEXT_LENGTH,
    NE_NODE,
    NE_OFFSETS,
)


async def import_named_entity_rows(
    neo4j_session: neo4j.AsyncSession,
    records: List[Dict],
    *,
    transaction_batch_size: int,
) -> neo4j.ResultSummary:
    # TODO: use apoc.periodic.iterate(parallel:true, iterateList:true}) to || and speed
    #  up import...
    query = f"""UNWIND $rows AS row
CALL {{
    WITH row
    MERGE (mention:{NE_NODE} {{{NE_ID}: row.{NE_ID}}})
    SET
        mention.{NE_CATEGORY} = row.{NE_CATEGORY},
        mention.{NE_DOC_ID} = row.{NE_DOC_ID},
        mention.{NE_EXTRACTOR} = row.{NE_EXTRACTOR},
        mention.{NE_EXTRACTOR_LANG} = row.{NE_EXTRACTOR_LANG},
        mention.{NE_MENTION} = row.{NE_MENTION},
        mention.{NE_MENTION_NORM} = row.{NE_MENTION_NORM},
        mention.{NE_MENTION_NORM_TEXT_LENGTH} =\
            toInteger(row.{NE_MENTION_NORM_TEXT_LENGTH}),
        mention.{NE_OFFSETS} = row.{NE_OFFSETS}
}} IN TRANSACTIONS OF $batchSize ROWS
"""
    res = await neo4j_session.run(query, rows=records, batchSize=transaction_batch_size)
    summary = await res.consume()
    return summary
