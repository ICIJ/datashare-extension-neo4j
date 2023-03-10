import asyncio
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from functools import cached_property
from typing import Any, AsyncGenerator, Callable, Dict, List, Mapping, Optional, TextIO

from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_scan

from neo4j_app.core.elasticsearch.utils import (
    ASC,
    DOC_,
    HITS,
    ID,
    INDEX,
    KEEP_ALIVE,
    MAX,
    PIT,
    QUERY,
    SEARCH_AFTER,
    SLICE,
    SORT,
    TOTAL,
    VALUE,
    match_all,
)
from neo4j_app.core.neo4j import write_neo4j_csv

logger = logging.getLogger(__name__)


class ESClient(AsyncElasticsearch):
    def __init__(
        self,
        project_index: str,
        *,
        pagination: int,
        keep_alive: str = "1m",
        max_concurrency: int = 5,
        **kwargs,
    ):
        self._project_index = project_index
        if pagination > 10000:
            raise ValueError("Elasticsearch doesn't support pages of size > 10000")
        self._pagination_size = pagination
        self._keep_alive = keep_alive
        self._max_concurrency = max_concurrency
        super().__init__(**kwargs)

    @cached_property
    def project_index(self) -> str:
        return self._project_index

    @cached_property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    @cached_property
    def pagination_size(self) -> int:
        return self._pagination_size

    @cached_property
    def keep_alive(self) -> str:
        return self._keep_alive

    async def search(self, **kwargs) -> Dict[str, Any]:
        # pylint: disable=arguments-differ
        if PIT not in kwargs and PIT not in kwargs.get("body", {}):
            kwargs = deepcopy(kwargs)
            kwargs[INDEX] = self.project_index
        return await super().search(**kwargs)

    async def _poll_search_pages(
        self, sort: Optional[List[Dict]] = None, size: Optional[int] = None, **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if sort is None:
            sort = f"{DOC_}:{ASC}"
        if size is None:
            size = self.pagination_size
        if not size:
            raise ValueError("size is expected to be > 0")
        res = await self.search(size=size, sort=sort, **kwargs)
        kwargs = deepcopy(kwargs)
        yield res
        total_hits = res[HITS][TOTAL][VALUE]
        page_hits = res[HITS][HITS]
        remaining = total_hits - len(page_hits)
        while remaining > 0:
            search_after = page_hits[-1][SORT]
            if "body" in kwargs:
                kwargs["body"][SEARCH_AFTER] = search_after
            else:
                kwargs[SEARCH_AFTER] = search_after
            res = await self.search(size=size, sort=sort, **kwargs)
            yield res
            page_hits = res[HITS][HITS]
            remaining -= len(page_hits)

    async def async_scan(
        self,
        query: Optional[Mapping[str, Any]],
        *,
        scroll: str,
        scroll_size: int,
        **kwargs,
    ) -> AsyncGenerator[Dict, None]:
        async for res in async_scan(
            self, query=query, scroll=scroll, size=scroll_size, **kwargs
        ):
            yield res

    @asynccontextmanager
    async def pit(self, *, keep_alive: str, **kwargs) -> AsyncGenerator[Dict, None]:
        pit_id = None
        try:
            pit = (
                await self.open_point_in_time(  # pylint: disable=unexpected-keyword-arg
                    index=self.project_index, keep_alive=keep_alive, **kwargs
                )
            )
            pit_id = pit[ID]
            yield pit
        finally:
            if pit_id is not None:
                await self.close_point_in_time(body={ID: pit_id})

    async def write_concurrently_neo4j_csv(
        self,
        query: Optional[Mapping[str, Any]],
        csv_f: TextIO,
        header: List[str],
        *,
        es_to_neo4j: Callable[[Dict[str, Any]], Dict[str, str]],
        concurrency: Optional[int] = None,
        keep_alive: Optional[str] = None,
    ) -> int:
        if concurrency is None:
            concurrency = self.max_concurrency
        if keep_alive is None:
            keep_alive = self.keep_alive
        # 1 slice is not supported...
        concurrency = max(concurrency, 2)
        async with self.pit(keep_alive=keep_alive) as pit:
            # Max should be at least 2
            bodies = (
                sliced_search_with_pit(
                    query,
                    pit=pit,
                    id_=i,
                    max_=concurrency,
                    keep_alive=keep_alive,
                )
                for i in range(concurrency)
            )
            lock = asyncio.Lock()
            tasks = (
                self._write_search_to_neo4j_csv_with_lock(
                    f=csv_f,
                    header=header,
                    lock=lock,
                    es_to_neo4j=es_to_neo4j,
                    body=body,
                )
                for body in bodies
            )
            total_hits = await asyncio.gather(*tasks)
            total_hits = sum(total_hits)
        return total_hits

    async def _write_search_to_neo4j_csv_with_lock(
        self,
        *,
        f: TextIO,
        header: List[str],
        lock: asyncio.Lock,
        es_to_neo4j: Callable[[Dict[str, Any]], Dict[str, str]],
        **kwargs,
    ) -> int:
        total_hits = 0
        async for res in self._poll_search_pages(**kwargs):
            total_hits += len(res[HITS][HITS])
            # Let's not lock the while converting the rows even if that implies using
            # some memory
            rows = [es_to_neo4j(hit) for hit in res[HITS][HITS]]
            async with lock:
                write_neo4j_csv(f, rows=rows, header=header, write_header=False)
                f.flush()
        return total_hits


def sliced_search_with_pit(
    query: Optional[Dict[str, Any]],
    *,
    pit: Dict[str, Any],
    id_: int,
    max_: int,
    keep_alive: Optional[str] = None,
) -> Dict:
    if query is None:
        query = {QUERY: match_all()}
    else:
        query = deepcopy(query)
    update = {PIT: pit, SLICE: {ID: id_, MAX: max_}}
    if keep_alive is not None:
        update[PIT][KEEP_ALIVE] = keep_alive
    query.update(update)
    return query
