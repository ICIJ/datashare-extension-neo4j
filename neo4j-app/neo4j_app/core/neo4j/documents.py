import csv
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import AsyncIterable, Dict, List, Optional, TextIO, Tuple

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


async def import_documents_from_csv_tx(
    tx: neo4j.AsyncTransaction, neo4j_import_path: Path
) -> neo4j.ResultSummary:
    # TODO: use apoc.periodic.iterate(.., ..., {batchSize:10000, parallel:true,
    #  iterateList:true}) to || and speed import...
    # TODO: if this is too long it might be worth skipping the ON MATCH part,
    #  if we do so properties updates will be disabled and in this case we'll want to
    #  have route to clean the DB and start fresh
    # TODO: add an index on document id
    query = f"""LOAD CSV WITH HEADERS FROM 'file:///{neo4j_import_path}' AS row
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


async def write_neo4j_csv(
    f: TextIO, *, rows: AsyncIterable[Dict], header: List[str]
) -> int:
    num_docs = 0
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
    async for row in rows:
        num_docs += 1
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
    return num_docs


@contextmanager
def make_neo4j_import_file(
    *, neo4j_import_dir: Path, neo4j_import_prefix: Optional[str]
) -> Tuple[tempfile.NamedTemporaryFile, Path]:
    try:
        with tempfile.NamedTemporaryFile(
            "w", dir=str(neo4j_import_dir), suffix=".csv"
        ) as import_file:
            neo4j_import_path = Path(import_file.name).name
            if neo4j_import_prefix is not None:
                neo4j_import_path = Path(neo4j_import_prefix).joinpath(
                    neo4j_import_path
                )
            yield import_file, neo4j_import_path
    finally:
        if Path(import_file.name).exists():
            os.remove(import_file.name)
