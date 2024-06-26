import hashlib
from typing import Any, Dict, List, Optional, TextIO
from urllib.parse import unquote_plus

from neo4j_app.constants import (
    DOC_COLUMNS,
    DOC_CONTENT_TYPE,
    DOC_CREATED_AT,
    DOC_CREATED_AT_META,
    DOC_EXTRACTION_LEVEL,
    DOC_ID,
    DOC_METADATA,
    DOC_MODIFIED_AT,
    DOC_MODIFIED_AT_META,
    DOC_NODE,
    DOC_PATH,
    DOC_ROOT_ID,
    DOC_TITLE,
    DOC_URL_SUFFIX,
    EMAIL_HEADER,
    EMAIL_RECEIVED_TYPE,
    EMAIL_REL_COLS,
    EMAIL_REL_HEADER_FIELDS,
    EMAIL_SENT_TYPE,
    NEO4J_ARRAY_SPLIT_CHAR,
    NEO4J_CSV_COL,
    NEO4J_CSV_END_ID,
    NEO4J_CSV_ID,
    NEO4J_CSV_LABEL,
    NEO4J_CSV_START_ID,
    NEO4J_CSV_TYPE,
    NE_APPEARS_IN_DOC,
    NE_APPEARS_IN_DOC_COLS,
    NE_CATEGORY,
    NE_COLUMNS,
    NE_DOC_ID,
    NE_EXTRACTORS,
    NE_ID,
    NE_IDS,
    NE_MENTION_NORM,
    NE_METADATA,
    NE_NODE,
    NE_OFFSETS,
    RECEIVED_EMAIL_HEADERS,
    SENT_EMAIL_HEADERS,
)
from neo4j_app.core.elasticsearch.utils import ID_, INDEX_, JOIN, PARENT, SOURCE
from neo4j_app.core.neo4j import write_neo4j_csv

_DS_DOC_URL = "ds/"


def es_to_neo4j_doc_row(document_hit: Dict) -> List[Dict[str, Any]]:
    doc_id = document_hit["_id"]
    doc = {DOC_ID: doc_id}
    hit_source = document_hit[SOURCE]
    doc.update({k: hit_source[k] for k in hit_source if k in DOC_COLUMNS})
    root_id = hit_source.get(DOC_ROOT_ID)
    if root_id is not None and root_id != doc_id:
        doc[DOC_ROOT_ID] = root_id
    doc_url = (
        f"{_DS_DOC_URL}{document_hit[INDEX_]}/{doc_id}/{doc.get(DOC_ROOT_ID, doc_id)}"
    )
    doc[DOC_URL_SUFFIX] = doc_url
    doc_title = _parse_doc_title(document_hit)
    doc[DOC_TITLE] = doc_title
    return [doc]


def _is_email(doc_hit: Dict) -> bool:
    content_type = doc_hit[SOURCE].get(DOC_CONTENT_TYPE, "")
    return (
        content_type.startswith("message/")
        or content_type == "application/vnd.ms-outlook"
    )


def _email_titles(doc_hit: Dict) -> List[str]:
    metadata = doc_hit[SOURCE].get(DOC_METADATA, {})
    titles = [metadata.get("tika_metadata_dc_title", "").strip()]
    subject = metadata.get(
        "tika_metadata_subject", metadata.get("tika_metadata_dc_subject", "")
    )
    titles.append(subject.strip())
    return titles


def _is_tweet(doc_hit: Dict) -> bool:
    content_type = doc_hit[SOURCE].get(DOC_CONTENT_TYPE, "")
    return content_type == "application/json; twint"


def _tweet_title(doc_hit: Dict) -> str:
    metadata = doc_hit[SOURCE].get(DOC_METADATA, dict())
    return metadata.get("tika_metadata_dc_title", "").strip()


def _short_doc_id(doc_hit: Dict) -> str:
    return doc_hit[ID_][:10]


def _doc_base_name(doc_hit: Dict) -> str:
    path = doc_hit[SOURCE].get(DOC_PATH, "")
    return path.split("/")[-1]


def _doc_resource_name(doc_hit: Dict) -> str:
    source = doc_hit[SOURCE]
    extraction_level = source.get(DOC_EXTRACTION_LEVEL, 0)
    if not extraction_level:
        return ""
    resource_name = (
        source.get(DOC_METADATA, dict()).get("tika_metadata_resourcename", "").strip()
    )
    if resource_name.startswith("=?") and resource_name.endswith("?="):
        resource_name = resource_name.split("?")[-2]
        resource_name = unquote_plus(resource_name.replace("=", "%"))
    return resource_name


def _doc_title(doc_hit: Dict) -> str:
    return doc_hit[SOURCE].get(DOC_TITLE, "").strip()


def _default_title(doc_hit: Dict) -> str:
    titles = [
        _short_doc_id(doc_hit),
        _doc_base_name(doc_hit),
        _doc_resource_name(doc_hit),
        _doc_title(doc_hit),
    ]
    for t in titles[::-1]:
        if t:
            return t
    raise ValueError("couldn't find any valid default title")


def _parse_doc_title(doc_hit: Dict) -> str:
    titles = [_default_title(doc_hit)]
    if _is_email(doc_hit):
        titles.extend(_email_titles(doc_hit))
    elif _is_tweet(doc_hit):
        titles.append(_tweet_title(doc_hit))
    for t in titles[::-1]:
        if t:
            return t
    raise ValueError("couldn't find any valid title")


def _coalesce(item: Dict[str, Any], columns: List[str]) -> Optional[Any]:
    for c in columns:
        value = item.get(c)
        if value is not None:
            return value
    return None


def es_to_neo4j_doc_csv(
    document_hit: Dict, *, prop_to_col_header: Dict[str, str]
) -> List[Dict[str, str]]:
    doc = es_to_neo4j_doc_row(document_hit)[0]
    doc.pop(DOC_ROOT_ID, None)
    metadata = doc.pop(DOC_METADATA, None)
    if metadata is not None:
        doc[DOC_CREATED_AT] = _coalesce(metadata, DOC_CREATED_AT_META)
        doc[DOC_MODIFIED_AT] = _coalesce(metadata, DOC_MODIFIED_AT_META)
    doc = {prop_to_col_header[prop]: value for prop, value in doc.items()}
    doc[NEO4J_CSV_LABEL] = DOC_NODE
    return [doc]


_DOC_ROOT_REL_START_COL = f"{NEO4J_CSV_START_ID}({DOC_NODE})"
_DOC_ROOT_REL_END_COL = f"{NEO4J_CSV_END_ID}({DOC_NODE})"


def es_to_neo4j_doc_root_rel_csv(document_hit: Dict) -> List[Dict[str, str]]:
    doc_id = document_hit["_id"]
    root_id = document_hit[SOURCE].get(DOC_ROOT_ID)
    if root_id is not None and root_id != doc_id:
        rel = {_DOC_ROOT_REL_START_COL: doc_id, _DOC_ROOT_REL_END_COL: root_id}
        return [rel]
    return []


def es_to_neo4j_named_entity_row(ne_hit: Dict) -> List[Dict[str, str]]:
    ent = {NE_ID: ne_hit["_id"]}
    hit_source = ne_hit[SOURCE]
    excluded = {JOIN}
    ent.update(
        {k: hit_source[k] for k in hit_source if k in NE_COLUMNS if k not in excluded}
    )
    ent[NE_DOC_ID] = hit_source[JOIN][PARENT]
    return [ent]


def make_ne_hit_id(*, mention_norm: str, category: str) -> str:
    id_gen = hashlib.sha256()
    # No ideal hashing but considered acceptable 🤷🏽
    key = (mention_norm, category)
    id_gen.update(str(key).encode("utf-8"))
    return id_gen.hexdigest()


def es_to_neo4j_named_entity_csv(
    ne_hit: Dict, *, prop_to_col_header: Dict[str, str]
) -> List[Dict[str, str]]:
    ne = {prop_to_col_header[NE_MENTION_NORM]: ne_hit[SOURCE][NE_MENTION_NORM]}
    label = f"{NE_NODE}{NEO4J_ARRAY_SPLIT_CHAR}{ne_hit[SOURCE][NE_CATEGORY]}"
    ne[NEO4J_CSV_LABEL] = label
    # We add an ID here since it's required by neo4j admin-import:
    # https://neo4j.com/docs/operations-manual/current/tutorial/neo4j-admin-import/
    # the ID won't be imported since we leave the property name blank
    ne[NEO4J_CSV_ID] = make_ne_hit_id(
        mention_norm=ne_hit[SOURCE][NE_MENTION_NORM],
        category=ne_hit[SOURCE][NE_CATEGORY],
    )
    return [ne]


_NE_DOC_REL_END_COL = f"{NEO4J_CSV_END_ID}({DOC_NODE})"


def es_to_neo4j_named_entity_doc_rel_csv(ne_hit: Dict) -> List[Dict]:
    rel = es_to_neo4j_named_entity_row(ne_hit)
    item = rel[0]
    item[NEO4J_CSV_START_ID] = make_ne_hit_id(
        mention_norm=ne_hit[SOURCE][NE_MENTION_NORM],
        category=ne_hit[SOURCE][NE_CATEGORY],
    )
    item[_NE_DOC_REL_END_COL] = item.pop(NE_DOC_ID)
    item[NEO4J_CSV_TYPE] = NE_APPEARS_IN_DOC
    return [item]


def es_to_neo4j_email_rel_csv(ne_hit: Dict) -> List[Optional[Dict[str, str]]]:
    item = es_to_neo4j_named_entity_doc_rel_csv(ne_hit)[0]
    metadata = item.get(NE_METADATA)
    if metadata is None:
        return [None]
    email_header = metadata.get(EMAIL_HEADER)
    if email_header is None:
        return [None]
    if email_header in RECEIVED_EMAIL_HEADERS:
        item[NEO4J_CSV_TYPE] = EMAIL_RECEIVED_TYPE
    elif email_header in SENT_EMAIL_HEADERS:
        item[NEO4J_CSV_TYPE] = EMAIL_SENT_TYPE
    else:
        return [None]
    return [item]


_NE_DOC_REL_OFFSETS_CSV_COL = (
    f"{NE_OFFSETS}:{NE_APPEARS_IN_DOC_COLS[NE_OFFSETS][NEO4J_CSV_COL]}"
)
_NE_DOC_REL_EXTRACTORS_CSV_COL = (
    f"{NE_EXTRACTORS}:{NE_APPEARS_IN_DOC_COLS[NE_EXTRACTORS][NEO4J_CSV_COL]}"
)
_NE_DOC_REL_IDS_CSV_COL = f"{NE_IDS}:{NE_APPEARS_IN_DOC_COLS[NE_IDS][NEO4J_CSV_COL]}"

_EMAIL_REL_FIELDS_CSV_COL = (
    f"{EMAIL_REL_HEADER_FIELDS}:"
    f"{EMAIL_REL_COLS[EMAIL_REL_HEADER_FIELDS][NEO4J_CSV_COL]}"
)


def write_es_rows_to_ne_doc_rel_csv(f: TextIO, rows: List[Dict], header: List[str]):
    for row in rows:
        offsets = row.pop(NE_OFFSETS)
        offsets = NEO4J_ARRAY_SPLIT_CHAR.join(str(off) for off in sorted(set(offsets)))
        row[_NE_DOC_REL_OFFSETS_CSV_COL] = offsets
        row[_NE_DOC_REL_EXTRACTORS_CSV_COL] = NEO4J_ARRAY_SPLIT_CHAR.join(
            sorted(set(row.pop(NE_EXTRACTORS)))
        )
        row[_NE_DOC_REL_IDS_CSV_COL] = NEO4J_ARRAY_SPLIT_CHAR.join(
            sorted(row.pop(NE_IDS))
        )
    write_neo4j_csv(f, rows=rows, header=header, write_header=False)


_EMAIL_REL_HEADER_FIELDS_CSV_COL = (
    f"{EMAIL_REL_HEADER_FIELDS}:"
    f"{EMAIL_REL_COLS[EMAIL_REL_HEADER_FIELDS][NEO4J_CSV_COL]}"
)


def write_es_rows_to_email_rel_csv(f: TextIO, rows: List[Dict], header: List[str]):
    written = []
    for row in rows:
        row[_EMAIL_REL_HEADER_FIELDS_CSV_COL] = NEO4J_ARRAY_SPLIT_CHAR.join(
            sorted(set(row.pop(EMAIL_REL_HEADER_FIELDS)))
        )
        written.append(row)
    if written:
        write_neo4j_csv(f, rows=written, header=header, write_header=False)
