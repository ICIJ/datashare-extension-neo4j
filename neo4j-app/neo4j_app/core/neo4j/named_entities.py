from pathlib import Path

import neo4j

from neo4j_app.constants import (
    NE_CATEGORY,
    NE_DOC_ID,
    NE_EXTRACTOR,
    NE_EXTRACTOR_LANG,
    NE_ID,
    NE_NODE,
    NE_MENTION,
    NE_MENTION_NORM,
    NE_MENTION_NORM_TEXT_LENGTH,
    NE_OFFSETS,
    NE_OFFSET_SPLITCHAR,
)


async def import_named_entities_from_csv_tx(
    tx: neo4j.AsyncTransaction, neo4j_import_path: Path
) -> neo4j.ResultSummary:
    # TODO: use apoc.periodic.iterate(.., ..., {batchSize:10000, parallel:true,
    #  iterateList:true}) to || and save memory import...
    query = f"""LOAD CSV WITH HEADERS FROM 'file:///{neo4j_import_path}' AS row
WITH row, \
[offset IN split(row.{NE_OFFSETS}, '{NE_OFFSET_SPLITCHAR}') | toInteger(offset)] \
as rowOffsets  
MERGE (mention:{NE_NODE} {{{NE_ID}: row.{NE_ID}}})
ON CREATE
    SET
        mention.{NE_CATEGORY} = row.{NE_CATEGORY},
        mention.{NE_DOC_ID} = row.{NE_DOC_ID},
        mention.{NE_EXTRACTOR} = row.{NE_EXTRACTOR},
        mention.{NE_EXTRACTOR_LANG} = row.{NE_EXTRACTOR_LANG},
        mention.{NE_MENTION} = row.{NE_MENTION},
        mention.{NE_MENTION_NORM} = row.{NE_MENTION_NORM},
        mention.{NE_MENTION_NORM_TEXT_LENGTH} =\
            toInteger(row.{NE_MENTION_NORM_TEXT_LENGTH}),
        mention.{NE_OFFSETS} = rowOffsets
ON MATCH
    SET 
        mention.{NE_CATEGORY} = row.{NE_CATEGORY},
        mention.{NE_DOC_ID} = row.{NE_DOC_ID},
        mention.{NE_EXTRACTOR} = row.{NE_EXTRACTOR},
        mention.{NE_EXTRACTOR_LANG} = row.{NE_EXTRACTOR_LANG},
        mention.{NE_MENTION} = row.{NE_MENTION},
        mention.{NE_MENTION_NORM} = row.{NE_MENTION_NORM},
        mention.{NE_MENTION_NORM_TEXT_LENGTH} =\
            toInteger(row.{NE_MENTION_NORM_TEXT_LENGTH}),
        mention.{NE_OFFSETS} = rowOffsets
"""
    res = await tx.run(query)
    summary = await res.consume()
    return summary
