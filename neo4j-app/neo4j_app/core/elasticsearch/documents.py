from typing import AsyncIterable, Dict, Generator

from neo4j_app.constants import DOC_COLUMNS
from neo4j_app.core.elasticsearch.utils import SOURCE


async def to_document_csv(
    document_hits: AsyncIterable[Dict],
) -> Generator[Dict[str, str], None, None]:
    async for doc in document_hits:
        yield _hit_to_row(doc)


def _hit_to_row(document_hit: Dict) -> Dict[str, str]:
    hit_source = document_hit[SOURCE]
    return {k: hit_source[k] for k in hit_source if k in DOC_COLUMNS}
