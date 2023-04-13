import abc
import asyncio
import logging
from contextlib import asynccontextmanager
from copy import copy, deepcopy
from functools import cached_property
from typing import Any, AsyncGenerator, Callable, Dict, List, Mapping, Optional, TextIO

import neo4j
from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_scan

from neo4j_app.core.elasticsearch.utils import (
    ASC,
    DOC_,
    HITS,
    ID,
    ID_,
    INDEX,
    KEEP_ALIVE,
    MAX,
    PIT,
    QUERY,
    SEARCH_AFTER,
    SLICE,
    SORT,
    and_query,
    ids_query,
    match_all,
)
from neo4j_app.core.neo4j import Neo4Import, Neo4jImportWorker, write_neo4j_csv
from neo4j_app.core.utils import batch
from neo4j_app.core.utils.asyncio import run_with_concurrency
from neo4j_app.core.utils.logging import log_elapsed_time_cm

logger = logging.getLogger(__name__)

PointInTime = Dict[str, Any]


class ESClientABC(metaclass=abc.ABCMeta):
    def __init__(
        self,
        project_index: str,
        *,
        pagination: int,
        keep_alive: str = "1m",
        max_concurrency: int = 5,
    ):
        # pylint: disable=super-init-not-called
        self._project_index = project_index
        if pagination > 10000:
            raise ValueError("Elasticsearch doesn't support pages of size > 10000")
        self._pagination_size = pagination
        self._keep_alive = keep_alive
        self._max_concurrency = max_concurrency

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
        page_hits = res[HITS][HITS]
        # TODO: find more elegant solution here, indeed if we know how many hits
        #  are left on the slice, we could avoid making the last call which returns
        #  empty results
        while page_hits:
            search_after = page_hits[-1][SORT]
            if "body" in kwargs:
                kwargs["body"][SEARCH_AFTER] = search_after
            else:
                kwargs[SEARCH_AFTER] = search_after
            res = await self.search(size=size, sort=sort, **kwargs)
            yield res
            page_hits = res[HITS][HITS]

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
    async def pit(
        self, *, keep_alive: str, **kwargs
    ) -> AsyncGenerator[PointInTime, None]:
        pit_id = None
        try:
            pit = await self.open_point_in_time(
                index=self.project_index, keep_alive=keep_alive, **kwargs
            )
            yield pit
        finally:
            if pit_id is not None:
                await self._close_pit(pit_id)

    async def to_neo4j(
        self,
        query: Optional[Mapping[str, Any]],
        *,
        pit: PointInTime,
        neo4j_import_worker_factory: Callable[[str], Neo4jImportWorker],
        num_neo4j_workers: int,
        concurrency: Optional[int] = None,
        max_records_in_memory: int,
        import_batch_size: int,
        keep_alive: Optional[str] = None,
        imported_entity_label: str,
    ) -> [List[str], List[neo4j.ResultSummary]]:
        if num_neo4j_workers <= 0:
            raise ValueError("num_neo4j_workers must be > 0")
        if concurrency is None:
            concurrency = self.max_concurrency
        if keep_alive is None:
            keep_alive = self.keep_alive
        logger.info(
            "Starting nodes import with %s neo4j and %s elasticsearch workers",
            num_neo4j_workers,
            concurrency,
        )
        queue_size = max_records_in_memory // import_batch_size
        if not queue_size:
            raise ValueError("import_batch_size must be >= max_records_in_memory")
        queue = asyncio.Queue(maxsize=queue_size)

        # Start the consumer tasks
        neo4j_workers = [
            neo4j_import_worker_factory(f"neo4j-node-worker-{i}")
            for i in range(num_neo4j_workers)
        ]
        neo4j_tasks = set(
            asyncio.create_task(worker(queue), name=worker.name)
            for worker in neo4j_workers
        )
        # To prevent keeping references to finished tasks forever, make each task remove
        # its own reference from the set after completion
        for task in neo4j_tasks:
            task.add_done_callback(neo4j_tasks.discard)
        # 1 slice is not supported...
        concurrency = max(concurrency, 2)
        bodies = [
            sliced_search_with_pit(
                query,
                pit=pit,
                id_=i,
                max_=concurrency,
                keep_alive=keep_alive,
            )
            for i in range(concurrency)
        ]
        with log_elapsed_time_cm(
            logger,
            logging.INFO,
            f"Retrieved all {imported_entity_label} from elasticsearch in"
            f" {{elapsed_time}} !",
        ):
            es_ids = await self._fill_import_queue(
                import_batch_size=import_batch_size,
                bodies=bodies,
                queue=queue,
            )
        # Wait for the queue to be fully consumed
        await queue.join()
        # Cancel consumer tasks which will make them break their infinite loop and
        # return the result summaries
        for task in neo4j_tasks:
            task.cancel()
        # Wait for all results to be there
        summaries = await asyncio.gather(*neo4j_tasks)
        summaries = sum(summaries, [])
        return es_ids, summaries

    async def to_neo4j_relationships(
        self,
        query: Optional[Mapping[str, Any]],
        ids: List[str],
        *,
        pit: PointInTime,
        neo4j_driver: neo4j.AsyncDriver,
        neo4j_import_fn: Neo4Import,
        to_neo4j_relationship: Optional[Callable[[Any], Optional[List[Dict]]]] = None,
        concurrency: Optional[int] = None,
        max_records_in_memory: int,
        import_batch_size: int,
        transaction_batch_size: int,
        keep_alive: Optional[str] = None,
        imported_entity_label: str,
    ) -> List[neo4j.ResultSummary]:
        if not ids:
            return []
        if concurrency is None:
            concurrency = self.max_concurrency
        if keep_alive is None:
            keep_alive = self.keep_alive
        logger.info(
            "Starting relationships import with %s elasticsearch workers",
            concurrency,
        )
        queue_size = max_records_in_memory // import_batch_size
        if not queue_size:
            raise ValueError("import_batch_size must be >= max_records_in_memory")
        queue = asyncio.Queue(maxsize=queue_size)

        worker = Neo4jImportWorker(
            name="neo4j-rel-worker",
            neo4j_driver=neo4j_driver,
            import_fn=neo4j_import_fn,
            transaction_batch_size=transaction_batch_size,
            to_neo4j=to_neo4j_relationship,
        )
        neo4j_tasks = {asyncio.create_task(worker(queue), name="neo4j-rel-worker")}
        # To prevent keeping references to finished tasks forever, make each task remove
        # its own reference from the set after completion
        next(iter(neo4j_tasks)).add_done_callback(neo4j_tasks.discard)
        # 1 slice is not supported...
        concurrency = max(concurrency, 2)
        # Since n_batch_ids = len(ids) / concurrency might huge, we don't want to send
        # this huge list through the network for each page. On the opposite, changing
        # the list of value each time prevents from benefiting from ES caching.
        # Taking that into account we decide to leverage the cache for 10 pages,
        # which should reasonably size list of values sent through the network
        es_batch_size = self._pagination_size * 10
        bodies = (
            search_by_id_with_pit(
                ids=id_batch,
                query=query,
                pit=pit,
                keep_alive=keep_alive,
            )
            for id_batch in batch(ids, batch_size=es_batch_size)
        )
        with log_elapsed_time_cm(
            logger,
            logging.INFO,
            f"Retrieved all {imported_entity_label} from elasticsearch in"
            f" {{elapsed_time}} !",
        ):
            await self._fill_import_queue(
                import_batch_size=import_batch_size,
                bodies=list(bodies),
                queue=queue,
                max_concurrency=concurrency,
            )
        # Wait for the queue to be fully consumed
        await queue.join()
        # Cancel consumer tasks which will make them break their infinite loop and
        # return the result summaries
        next(iter(neo4j_tasks)).cancel()
        # Wait for all results to be there
        summaries = await asyncio.gather(*neo4j_tasks)
        summaries = sum(summaries, [])
        return summaries

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

    async def _fill_import_queue(
        self,
        queue: asyncio.Queue,
        import_batch_size: int,
        bodies: List[Dict],
        max_concurrency: Optional[int] = None,
    ) -> List[str]:
        lock = asyncio.Lock()
        buffer = []
        futures = [
            self._fill_import_buffer(
                queue=queue,
                import_batch_size=import_batch_size,
                lock=lock,
                buffer=buffer,
                body=body,
            )
            for body in bodies
        ]
        if max_concurrency is None:
            ids = await asyncio.gather(*futures)
        else:
            ids = [
                res
                async for res in run_with_concurrency(
                    futures, max_concurrency=max_concurrency
                )
            ]
        ids = sum(ids, [])
        if buffer:
            ids += [rec[ID_] for rec in buffer]
            await _enqueue_import_batch(queue, buffer)
        return ids

    async def _fill_import_buffer(
        self,
        *,
        queue: asyncio.Queue,
        import_batch_size: int,
        lock: asyncio.Lock,
        buffer: List[Dict],
        **kwargs,
    ) -> List[str]:
        # TODO: use https://github.com/jd/tenacity to implement retry with backoff in
        #  case of network error
        ids = []
        # Since we can't provide an async generator to the neo4j client, let's store
        # results into a list fitting in memory, which will then be split into
        # transaction batches
        async for res in self._poll_search_pages(**kwargs):
            buffer.extend(res[HITS][HITS])
            async with lock:
                if len(buffer) >= import_batch_size:
                    await _enqueue_import_batch(queue, buffer)
                    ids += [rec[ID_] for rec in buffer]
                    buffer.clear()
        return ids

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


async def _enqueue_import_batch(queue: asyncio.Queue, import_batch: List[Dict]):
    # Let's monitor the time it takes to enqueue the batch, if it's long it
    # means that more neo4j workers are needed
    with log_elapsed_time_cm(
        logger, logging.DEBUG, "Waited {elapsed_time} to enqueue batch"
    ):
        await queue.put(copy(import_batch))


class ESClient(ESClientABC, AsyncElasticsearch):
    def __init__(
        self,
        project_index: str,
        *,
        pagination: int,
        keep_alive: str = "1m",
        max_concurrency: int = 5,
        **kwargs,
    ):
        ESClientABC.__init__(
            self,
            project_index=project_index,
            pagination=pagination,
            keep_alive=keep_alive,
            max_concurrency=max_concurrency,
        )
        AsyncElasticsearch.__init__(self, **kwargs)

    # TODO: this should be class attr
    @cached_property
    def _pit_id(self) -> str:
        return ID

    async def _close_pit(self, pit_id: str):
        await self.close_point_in_time(body={ID: pit_id})


try:
    from opensearchpy import AsyncOpenSearch

    class OSClient(ESClientABC, AsyncOpenSearch):
        def __init__(
            self,
            project_index: str,
            *,
            pagination: int,
            keep_alive: str = "1m",
            max_concurrency: int = 5,
            **kwargs,
        ):
            ESClientABC.__init__(
                self,
                project_index=project_index,
                pagination=pagination,
                keep_alive=keep_alive,
                max_concurrency=max_concurrency,
            )
            AsyncOpenSearch.__init__(self, **kwargs)

        # TODO: this should be class attr
        @cached_property
        def _pit_id(self) -> str:
            return "pid_id"

        async def open_point_in_time(
            self, index: str, keep_alive: str, **kwargs
        ) -> Dict:
            pit = await self.create_point_in_time(  # pylint: disable=unexpected-keyword-arg
                index=index,
                keep_alive=keep_alive,
                **kwargs,
            )
            pit = {ID: pit["pit_id"]}
            return pit

        async def _close_pit(self, pit_id: str):
            await self.close_point_in_time(body={"pid_id": [pit_id]})

except ImportError:
    pass


def sliced_search_with_pit(
    query: Optional[Dict[str, Any]],
    *,
    pit: PointInTime,
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


def search_by_id_with_pit(
    ids: List[str],
    query: Optional[Dict[str, Any]] = None,
    *,
    pit: PointInTime,
    keep_alive: Optional[str] = None,
) -> Dict:
    ids_query_ = ids_query(ids)
    if query is not None:
        query = and_query(query, ids_query_)
    else:
        query = {QUERY: ids_query_}
    update = {PIT: pit}
    if keep_alive is not None:
        update[PIT][KEEP_ALIVE] = keep_alive
    query.update(update)
    return query
