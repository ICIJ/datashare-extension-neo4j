from typing import Dict, List, Tuple

import neo4j

from neo4j_app.constants import (
    DOC_ID,
    DOC_NODE,
    EMAIL_HEADER,
    EMAIL_RECEIVED_REL_LABEL,
    EMAIL_REL_HEADER_FIELDS,
    EMAIL_SENT_REL_LABEL,
    NE_APPEARS_IN_DOC,
    NE_CATEGORY,
    NE_DOC_ID,
    NE_EXTRACTOR,
    NE_EXTRACTORS,
    NE_EXTRACTOR_LANG,
    NE_ID,
    NE_IDS,
    NE_MENTION_NORM,
    NE_METADATA,
    NE_NODE,
    NE_OFFSETS,
)

# TODO: check that this list is exhaustive, we know it isn't !!!
_SENT_EMAIL_HEADERS = ["tika_metadata_message_from"]
# TODO: check that this list is exhaustive, we know it isn't !!!
_RECEIVED_EMAIL_HEADERS = [
    "tika_metadata_message_bcc",
    "tika_metadata_message_cc",
    "tika_metadata_message_to",
]

_MERGE_SENT_EMAIL = f"""MERGE (mention)-[emailRel:{EMAIL_SENT_REL_LABEL}]->(doc)
ON CREATE
    SET emailRel.{EMAIL_REL_HEADER_FIELDS} = [row.{NE_METADATA}.{EMAIL_HEADER}]
ON MATCH
    SET emailRel.{EMAIL_REL_HEADER_FIELDS} =  apoc.coll.toSet(emailRel.{EMAIL_REL_HEADER_FIELDS} + row.{NE_METADATA}.{EMAIL_HEADER})
RETURN emailRel"""

_MERGE_RECEIVED_EMAIL = f"""MERGE (mention)-[emailRel:{EMAIL_RECEIVED_REL_LABEL}]->(doc)
ON CREATE
    SET emailRel.{EMAIL_REL_HEADER_FIELDS} = [row.{NE_METADATA}.{EMAIL_HEADER}]
ON MATCH
    SET emailRel.{EMAIL_REL_HEADER_FIELDS} =  apoc.coll.toSet(emailRel.{EMAIL_REL_HEADER_FIELDS} + row.{NE_METADATA}.{EMAIL_HEADER})
RETURN emailRel"""


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
    WITH mention, doc, row
    CALL apoc.do.case(
        [
            row.{NE_METADATA} IS NOT NULL AND row.{NE_METADATA}.{EMAIL_HEADER} IN $sentHeaders, '{_MERGE_SENT_EMAIL}',
            row.{NE_METADATA} IS NOT NULL AND row.{NE_METADATA}.{EMAIL_HEADER} IN $receivedHeaders, '{_MERGE_RECEIVED_EMAIL}'
        ],
        'RETURN NULL as emailRel',
      {{
        mention: mention,
        doc: doc,
        row: row
      }}
    ) YIELD value
    WITH value AS ignored, mention
    RETURN mention
}} IN TRANSACTIONS OF $batchSize ROWS
RETURN mention 
"""
    res = await neo4j_session.run(
        query,
        rows=records,
        batchSize=transaction_batch_size,
        sentHeaders=_SENT_EMAIL_HEADERS,
        receivedHeaders=_RECEIVED_EMAIL_HEADERS,
    )
    summary = await res.consume()
    return summary


async def ne_creation_stats_tx(tx: neo4j.AsyncTransaction) -> Tuple[int, int]:
    query = f"""MATCH (mention:{NE_NODE})
WITH count(mention) as numMentions
OPTIONAL MATCH (:{NE_NODE})-[rel:{NE_APPEARS_IN_DOC}]->(:{DOC_NODE})
RETURN numMentions, count(rel) as numRels
"""
    res = await tx.run(query)
    count = await res.single()
    if count is None:
        return 0, 0
    node_count = count["numMentions"]
    rel_count = count["numRels"]
    return node_count, rel_count
