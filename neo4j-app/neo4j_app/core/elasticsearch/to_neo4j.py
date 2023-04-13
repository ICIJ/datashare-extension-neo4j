from typing import Dict, List, Optional

from neo4j_app.constants import (
    DOC_COLUMNS,
    DOC_ID,
    DOC_ROOT_ID,
    NE_COLUMNS,
    NE_DOC_ID,
    NE_ID,
    NE_MENTION_NORM,
    NE_OFFSETS,
)
from neo4j_app.core.elasticsearch.utils import JOIN, PARENT, SOURCE


def es_to_neo4j_row(document_hit: Dict) -> List[Dict[str, str]]:
    doc = {DOC_ID: document_hit["_id"]}
    hit_source = document_hit[SOURCE]
    doc.update({k: hit_source[k] for k in hit_source if k in DOC_COLUMNS})
    return [doc]


def es_to_neo4j_doc_root_relationship(
    document_hit: Dict,
) -> Optional[List[Dict[str, str]]]:
    hit_source = document_hit[SOURCE]
    root_id = hit_source.get(DOC_ROOT_ID)
    if root_id is not None:
        rel = [{"child": document_hit["_id"], "root": root_id}]
        return rel
    return None


def es_to_neo4j_named_entity(ne_hit: Dict) -> List[Dict[str, str]]:
    ent = {NE_ID: ne_hit["_id"]}
    hit_source = ne_hit[SOURCE]
    excluded = {JOIN}
    ent.update(
        {k: hit_source[k] for k in hit_source if k in NE_COLUMNS if k not in excluded}
    )
    ent[NE_DOC_ID] = hit_source[JOIN][PARENT]
    return [ent]


def es_to_neo4j_ne_doc_relationship(ne_hit: Dict) -> Optional[List[Dict[str, str]]]:
    hit_source = ne_hit[SOURCE]
    doc_id = hit_source[NE_DOC_ID]
    mention_norm = hit_source[NE_MENTION_NORM]
    offsets = hit_source[NE_OFFSETS]
    rel = {"document_id": doc_id, NE_MENTION_NORM: mention_norm, NE_OFFSETS: offsets}
    return [rel]
