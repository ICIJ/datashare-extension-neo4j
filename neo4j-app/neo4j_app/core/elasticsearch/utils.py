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
FIELD = "field"
FUNCTION_SCORE = "function_score"
HAS_PARENT = "has_parent"
HITS = "hits"
ID_ = "_id"
ID = "id"
IDS = "ids"
INDEX = "index"
INDEX_ = "_index"
KEEP_ALIVE = "keep_alive"
JOIN = "join"
MATCH_ALL = "match_all"
MAX = "max"
MUST = "must"
PARENT = "parent"
PARENT_TYPE = "parent_type"
PIT = "pit"
QUERY = "query"
SCORE = "score"
SCORE_ = "_score"
SCORE_MODE = "SCORE_MODE"
SCRIPT = "script"
SCRIPT_SCORE = "script_score"
SCROLL = "scroll"
SCROLL_ID = "scroll_id"
SCROLL_ID_ = "_scroll_id"
SIZE = "size"
SEARCH_AFTER = "search_after"
SEED = "seed"
SHARD_DOC_ = "_shard_doc"
SLICE = "slice"
SORT = "sort"
SOURCE = "_source"
TERM = "term"
TOTAL = "total"
VALUE = "value"
VALUES = "values"


def and_query(*queries: Dict) -> Dict:
    return {QUERY: {BOOL: {MUST: list(queries)}}}


def with_sort(*, query: Dict, sort: Dict) -> Dict:
    if SORT not in query:
        query[SORT] = []
    return query[SORT].append(sort)


def with_source(query: Dict, sources: List[str]) -> Dict:
    query[SOURCE] = sources
    return query


def has_type(*, type_field: str, type_value: str) -> Dict:
    return {TERM: {type_field: type_value}}


def has_id(ids: List[str]) -> Dict:
    return {IDS: {VALUES: ids}}


def function_score(*, query: Dict, **kwargs) -> Dict:
    query = {FUNCTION_SCORE: {QUERY: query}}
    if kwargs:
        query[FUNCTION_SCORE].update(kwargs)
    return query


def has_parent(parent_type: str, query: Dict, *, score: bool = False) -> Dict:
    query = {HAS_PARENT: {PARENT_TYPE: parent_type, QUERY: query}}
    if score:
        query[HAS_PARENT][SCORE] = True
    return query


def match_all_query() -> Dict:
    return {QUERY: match_all()}


def match_all() -> Dict:
    return {MATCH_ALL: {}}


def ids_query(ids: List[str]) -> Dict:
    return {IDS: {VALUES: ids}}


def get_scroll_id(res: Dict) -> str:
    scroll_id = res.get(SCROLL_ID_)
    if scroll_id is None:
        msg = "Missing scroll ID, this response is probably not from a scroll search"
        raise ValueError(msg)
    return scroll_id


def get_total_hits(res: Dict) -> int:
    return res[HITS][TOTAL][VALUE]
