import logging
from typing import Dict


logger = logging.getLogger(__name__)

DEFAULT_SCROLL_DURATION = "1m"
HITS = "hits"
MATCH_ALL = "match_all"
QUERY = "query"
SCROLL_ID = "_scroll_id"
TOTAL = "total"
VALUE = "value"


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
