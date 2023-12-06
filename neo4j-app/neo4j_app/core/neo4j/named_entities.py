import logging
from typing import Dict, List, Tuple

import neo4j

from neo4j_app.constants import (
    DOC_ID,
    DOC_NODE,
    EMAIL_CATEGORY,
    EMAIL_DOMAIN,
    EMAIL_HEADER,
    EMAIL_RECEIVED_TYPE,
    EMAIL_REL_HEADER_FIELDS,
    EMAIL_SENT_TYPE,
    EMAIL_USER,
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
    RECEIVED_EMAIL_HEADERS,
    SENT_EMAIL_HEADERS,
)
from neo4j_app.typing_ import LightCounters

logger = logging.getLogger(__name__)

_MERGE_SENT_EMAIL = f"""MERGE (mention)-[emailRel:{EMAIL_SENT_TYPE}]->(doc)
ON CREATE
    SET emailRel.{EMAIL_REL_HEADER_FIELDS} = [row.{NE_METADATA}.{EMAIL_HEADER}]
ON MATCH
    SET emailRel.{EMAIL_REL_HEADER_FIELDS} =  apoc.coll.toSet(\
        emailRel.{EMAIL_REL_HEADER_FIELDS} + row.{NE_METADATA}.{EMAIL_HEADER})
RETURN emailRel"""

_MERGE_RECEIVED_EMAIL = f"""MERGE (mention)-[emailRel:{EMAIL_RECEIVED_TYPE}]->(doc)
ON CREATE
    SET emailRel.{EMAIL_REL_HEADER_FIELDS} = [row.{NE_METADATA}.{EMAIL_HEADER}]
ON MATCH
    SET emailRel.{EMAIL_REL_HEADER_FIELDS} =  apoc.coll.toSet(\
    emailRel.{EMAIL_REL_HEADER_FIELDS} + row.{NE_METADATA}.{EMAIL_HEADER})
RETURN emailRel"""

_MAKE_EMAIL_USER = f"""CASE mention.{NE_CATEGORY} = '{EMAIL_CATEGORY}'
  WHEN true THEN split(mention)
  ELSE null
END"""

_SET_EMAIL_USER_AND_DOMAIN = f"""WITH mention, row, \
        CASE WHEN row.{NE_CATEGORY} = '{EMAIL_CATEGORY}' \
            THEN split(mention.{NE_MENTION_NORM}, '@') \
            ELSE [] END as emailSplit
    SET
        mention.{EMAIL_USER} = CASE WHEN size(emailSplit) = 2 \
            THEN emailSplit[0] ELSE NULL END,
        mention.{EMAIL_DOMAIN} = CASE WHEN size(emailSplit) = 2 \
            THEN emailSplit[1] ELSE NULL END"""


async def import_named_entity_rows(
    neo4j_session: neo4j.AsyncSession,
    records: List[Dict],
    *,
    transaction_batch_size: int,
) -> LightCounters:
    # TODO: see if we can avoid the apoc.coll.toSet
    query = f"""UNWIND $rows AS row
CALL {{
    WITH row
    CALL apoc.merge.node(\
        ["{NE_NODE}", row.{NE_CATEGORY}], {{{NE_MENTION_NORM}: row.{NE_MENTION_NORM}}}\
    ) YIELD node as mention
    {_SET_EMAIL_USER_AND_DOMAIN}
    WITH mention, row
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
            rel.{NE_EXTRACTORS} = apoc.coll.toSet(\
                rel.{NE_EXTRACTORS} + row.{NE_EXTRACTOR}),
            rel.{NE_OFFSETS} = apoc.coll.toSet(rel.{NE_OFFSETS} + row.{NE_OFFSETS})
    WITH mention, doc, row
    CALL apoc.do.case(
        [
            row.{NE_METADATA} IS NOT NULL 
                AND row.{NE_METADATA}.{EMAIL_HEADER} IS NOT NULL
                AND row.{NE_METADATA}.{EMAIL_HEADER} IN $sentHeaders, \
                    '{_MERGE_SENT_EMAIL}',
            row.{NE_METADATA} IS NOT NULL 
                AND row.{NE_METADATA}.{EMAIL_HEADER} IS NOT NULL 
                AND row.{NE_METADATA}.{EMAIL_HEADER} IN $receivedHeaders, \
                    '{_MERGE_RECEIVED_EMAIL}'
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
        sentHeaders=list(SENT_EMAIL_HEADERS),
        receivedHeaders=list(RECEIVED_EMAIL_HEADERS),
    )
    summary = await res.consume()
    counters = LightCounters(
        nodes_created=summary.counters.nodes_created,
        relationships_created=summary.counters.relationships_created,
    )
    return counters


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
