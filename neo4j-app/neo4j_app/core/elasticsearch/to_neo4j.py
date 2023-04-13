from typing import Dict, List

from neo4j_app.constants import (
    DOC_COLUMNS,
    DOC_ID,
    NE_COLUMNS,
    NE_DOC_ID,
    NE_ID,
)
from neo4j_app.core.elasticsearch.utils import JOIN, PARENT, SOURCE


def es_to_neo4j_doc_row(document_hit: Dict) -> List[Dict[str, str]]:
    doc = {DOC_ID: document_hit["_id"]}
    hit_source = document_hit[SOURCE]
    doc.update({k: hit_source[k] for k in hit_source if k in DOC_COLUMNS})
    return [doc]


def es_to_neo4j_named_entity_row(ne_hit: Dict) -> List[Dict[str, str]]:
    ent = {NE_ID: ne_hit["_id"]}
    hit_source = ne_hit[SOURCE]
    excluded = {JOIN}
    ent.update(
        {k: hit_source[k] for k in hit_source if k in NE_COLUMNS if k not in excluded}
    )
    ent[NE_DOC_ID] = hit_source[JOIN][PARENT]
    return [ent]
