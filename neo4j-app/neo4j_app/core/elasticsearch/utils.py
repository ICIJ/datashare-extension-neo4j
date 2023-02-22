import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

ASC = "asc"
BOOL = "bool"
COUNT = "count"
DEFAULT_SCROLL_DURATION = "1m"
DOC_ = "_doc"
ES_DOCUMENT_TYPE = "Document"
ES_NAMED_ENTITY_TYPE = "NamedEntity"
HAS_PARENT = "has_parent"
HITS = "hits"
ID_ = "_id"
ID = "id"
IDS = "ids"
INDEX = "index"
KEEP_ALIVE = "keep_alive"
JOIN = "join"
MATCH_ALL = "match_all"
MAX = "max"
MUST = "must"
PARENT = "parent"
PARENT_TYPE = "parent_type"
PIT = "pit"
QUERY = "query"
SCROLL_ID = "_scroll_id"
SEARCH_AFTER = "search_after"
SLICE = "slice"
SORT = "sort"
SOURCE = "_source"
TERM = "term"
TOTAL = "total"
VALUE = "value"
VALUES = "values"


def and_query(*queries: Dict) -> Dict:
    return {QUERY: {BOOL: {MUST: list(queries)}}}


def has_type(*, type_field: str, type_value: str) -> Dict:
    return {TERM: {type_field: type_value}}


def has_id(ids: List[str]) -> Dict:
    return {IDS: {VALUES: ids}}


def has_parent(parent_type: str, query: Dict) -> Dict:
    return {HAS_PARENT: {PARENT_TYPE: parent_type, QUERY: query}}


def match_all_query() -> Dict:
    return {QUERY: match_all()}


def match_all() -> Dict:
    return {MATCH_ALL: {}}


def get_scroll_id(res: Dict) -> str:
    scroll_id = res.get(SCROLL_ID)
    if scroll_id is None:
        msg = "Missing scroll ID, this response is probably not from a scroll search"
        raise ValueError(msg)
    return scroll_id


def get_total_hits(res: Dict) -> int:
    return res[HITS][TOTAL][VALUE]
