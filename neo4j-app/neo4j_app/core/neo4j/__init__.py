import csv
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, List, Optional, TextIO, Tuple

from .imports import Neo4Import, Neo4jImportWorker
from .migrations import Migration
from .migrations.migrate import MigrationError, migrate_db_schemas
from .migrations.migrations import (
    migration_v_0_1_0_tx,
    migration_v_0_2_0_tx,
    migration_v_0_3_0_tx,
)

V_0_1_0 = Migration(
    version="0.1.0",
    label="Create migration and project and constraints",
    migration_fn=migration_v_0_1_0_tx,
)
V_0_2_0 = Migration(
    version="0.2.0",
    label="Create doc and named entities index and constraints",
    migration_fn=migration_v_0_2_0_tx,
)
V_0_3_0 = Migration(
    version="0.3.0",
    label="Create tasks indexes and constraints",
    migration_fn=migration_v_0_3_0_tx,
)
MIGRATIONS = [V_0_1_0, V_0_2_0, V_0_3_0]


def get_neo4j_csv_reader(
    f: TextIO, fieldnames: Optional[List[str]] = None
) -> csv.DictReader:
    writer = csv.DictReader(
        f,
        fieldnames=fieldnames,
        dialect="excel",
        doublequote=True,
        escapechar=None,
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )
    return writer


def get_neo4j_csv_writer(f: TextIO, header: List[str]) -> csv.DictWriter:
    writer = csv.DictWriter(
        f,
        fieldnames=header,
        dialect="excel",
        doublequote=True,
        escapechar=None,
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )
    return writer


def write_neo4j_csv(
    f: TextIO, *, rows: Iterable[Dict], header: List[str] = None, write_header: bool
) -> int:
    num_docs = 0
    writer = get_neo4j_csv_writer(f, header=header)
    if write_header:
        writer.writeheader()
    neo4j_escape_char = "\\"
    # Let's escape "\" if it's contained in a string value
    for row in rows:
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
    import_file = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", dir=str(neo4j_import_dir), suffix=".csv"
        ) as import_file:
            neo4j_import_path = Path(import_file.name).name
            if neo4j_import_prefix is None:
                neo4j_import_prefix = Path(".")
            neo4j_import_path = Path(neo4j_import_prefix).joinpath(neo4j_import_path)
            # Make import file accessible to neo4j
            os.chmod(import_file.name, 0o777)
            yield import_file, neo4j_import_path
    finally:
        if import_file is not None:
            if Path(import_file.name).exists():
                os.remove(import_file.name)
