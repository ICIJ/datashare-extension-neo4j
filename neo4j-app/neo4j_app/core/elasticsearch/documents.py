from typing import AsyncIterable, Dict, Generator

from neo4j_app.constants import DOC_COLUMNS, DOC_DOC_ID
from neo4j_app.core.elasticsearch.utils import SOURCE


async def to_document_csv(
    document_hits: AsyncIterable[Dict],
) -> Generator[Dict[str, str], None, None]:
    async for doc in document_hits:
        yield _hit_to_row(doc)


def _hit_to_row(document_hit: Dict) -> Dict[str, str]:
    doc = {DOC_DOC_ID: document_hit["_id"]}
    hit_source = document_hit[SOURCE]
    doc.update({k: hit_source[k] for k in hit_source if k in DOC_COLUMNS})
    return doc
