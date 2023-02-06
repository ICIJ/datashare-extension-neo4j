import csv
from pathlib import Path
from typing import Dict, Iterable, List, TextIO

import neo4j

from neo4j_app.constants import (
    DOC_CONTENT_LENGTH,
    DOC_CONTENT_TYPE,
    DOC_DIRNAME,
    DOC_DOC_ID,
    DOC_EXTRACTION_DATE,
    DOC_LABEL,
    DOC_PATH,
    DOC_ROOT_ID,
)


# TODO: here we're importing documents from a CSV file to simplify things a bit
async def import_documents_from_csv_tx(
    tx: neo4j.AsyncTransaction, csv_path: Path
) -> neo4j.ResultSummary:
    # TODO: if this is too long it might be worth skipping the ON MATCH part,
    #  if we do so properties updates will be disabled and in this case we'll want to
    #  have route to clean the DB and start fresh

    # TODO: add an index on document id
    query = f"""LOAD CSV WITH HEADERS FROM 'file:///{csv_path}' AS row
MERGE (doc:{DOC_LABEL} {{{DOC_DOC_ID}: row.{DOC_DOC_ID}}})
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


def write_neo4j_csv(f: TextIO, *, rows: Iterable[Dict[str, str]], header: List[str]):
    writer = csv.DictWriter(
        f,
        dialect="excel",
        doublequote=True,
        escapechar=None,
        quoting=csv.QUOTE_MINIMAL,
        fieldnames=header,
        lineterminator="\n",
    )
    writer.writeheader()
    neo4j_escape_char = "\\"
    # Let's escape "\" if it's contained in a string value
    for row in rows:
        formatted_row = dict()
        for k, v in row.items():
            if v is None:
                v = ""
            elif isinstance(v, str):
                v = v.replace(
                    neo4j_escape_char, f"{neo4j_escape_char}{neo4j_escape_char}"
                )
            formatted_row[k] = v
        writer.writerow(formatted_row)
