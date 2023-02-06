from typing import Dict, Generator, Iterable

from neo4j_app.constants import DOC_COLUMNS


def to_document_csv(
    document_hits: Iterable[Dict],
) -> Generator[Dict[str, str], None, None]:
    for doc in document_hits:
        yield _hit_to_row(doc)


def _hit_to_row(document_hit: Dict) -> Dict[str, str]:
    return {k: document_hit[k] for k in document_hit if k in DOC_COLUMNS}
