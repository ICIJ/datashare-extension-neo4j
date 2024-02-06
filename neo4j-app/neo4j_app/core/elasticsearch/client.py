import abc
import asyncio
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from distutils.version import StrictVersion
from functools import cached_property
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Collection,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    TextIO,
    Tuple,
    Union,
)

from elasticsearch import AsyncElasticsearch, TransportError
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    before_sleep_log,
    retry_base,
    stop_after_attempt,
    wait_random_exponential,
)

from neo4j_app.core.elasticsearch.utils import (
    ASC,
    COUNT,
    DOC_,
    HITS,
    ID,
    KEEP_ALIVE,
    MAX,
    PIT,
    QUERY,
    SCROLL,
    SCROLL_ID,
    SCROLL_ID_,
    SEARCH_AFTER,
    SHARD_DOC_,
    SIZE,
    SLICE,
    SORT,
    SOURCE,
    match_all,
)
from neo4j_app.core.neo4j import Neo4jImportWorker, write_neo4j_csv
from neo4j_app.core.utils.asyncio import iterate_with_concurrency, run_with_concurrency
from neo4j_app.core.utils.logging import log_elapsed_time_cm
from neo4j_app.core.utils.progress import to_raw_progress
from neo4j_app.typing_ import LightCounters, PercentProgress

logger = logging.getLogger(__name__)

PointInTime = Dict[str, Any]

_ES_PIT_VERSION = StrictVersion("7.11")
_OS_PIT_VERSION = StrictVersion("1.3")


class ESClientABC(metaclass=abc.ABCMeta):
    min_pit_version: StrictVersion
    _recoverable_error_codes: Set[int] = {429}

    def __init__(
        self,
        *,
        pagination: int,
        keep_alive: str = "1m",
        max_concurrency: int = 5,
        max_retries: int = 0,
        max_retry_wait_s: Union[int, float] = 60,
    ):
        # pylint: disable=super-init-not-called
        if pagination > 10000:
            raise ValueError("Elasticsearch doesn't support pages of size > 10000")
        self._pagination_size = pagination
        self._keep_alive = keep_alive
        self._max_concurrency = max_concurrency
        self._max_retries = max_retries
        self._max_retry_wait_s = max_retry_wait_s
        self._version: Optional[StrictVersion] = None

    @cached_property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    @cached_property
    def pagination_size(self) -> int:
        return self._pagination_size

    @cached_property
    def keep_alive(self) -> str:
        return self._keep_alive

    @property
    async def version(self) -> StrictVersion:
        if self._version is None:
            info = await self.info()
            self._version = StrictVersion(info["version"]["number"])
        return self._version

    @property
    async def supports_pit(self) -> bool:
        return await self.version > type(self).min_pit_version

    @abc.abstractmethod
    def default_sort(self, pit_search: bool) -> str:
        pass

    @abc.abstractmethod
    async def search(self, body: Optional[Dict], index: Optional[str], **kwargs):
        pass

    def _async_retrying(self) -> AsyncRetrying:
        return AsyncRetrying(
            wait=wait_random_exponential(max=self._max_retry_wait_s),
            stop=stop_after_attempt(self._max_retries),
            retry=_retry_if_error_code(self._recoverable_error_codes),
            before_sleep=before_sleep_log(logger, logging.ERROR),
            reraise=True,
        )

    async def poll_search_pages(
        self, index: str, body: Dict, **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if await self.supports_pit:
            results = self._poll_pit_search_pages(body=body, **kwargs)
        else:
            scroll = kwargs.pop(SCROLL, self.keep_alive)
            results = self._poll_scroll_search_pages(
                index=index, query=body, scroll=scroll, **kwargs
            )
        async for res in results:
            yield res

    async def _poll_pit_search_pages(
        self,
        sort: Optional[List[Dict]] = None,
        **kwargs,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        retrying = self._async_retrying()
        if sort is None:
            sort = self.default_sort(pit_search=True)
        res = await retrying(self.search, index=None, sort=sort, **kwargs)
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
            res = await retrying(self.search, index=None, sort=sort, **kwargs)
            page_hits = res[HITS][HITS]
            if page_hits:
                yield res

    async def _poll_scroll_search_pages(
        self,
        index: str,
        query: Dict,
        scroll: str,
        sort: Optional[List[Dict]] = None,
        **kwargs,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        retrying = self._async_retrying()
        if sort is None:
            sort = self.default_sort(pit_search=False)
        res = await retrying(
            self.search,
            index=index,
            body=query,
            scroll=scroll,
            sort=sort,
            **kwargs,
        )
        scroll_id = res.get(SCROLL_ID_)
        try:
            while scroll_id and res[HITS][HITS]:
                yield res
                res = await retrying(
                    self.scroll, body={SCROLL_ID: scroll_id, SCROLL: scroll}
                )
                scroll_id = res.get(SCROLL_ID_)
        finally:
            await self.clear_scroll(
                body={SCROLL_ID: [scroll_id]},
                ignore=(404,),
                params={"__elastic_client_meta": (("h", "s"),)},
            )

    @asynccontextmanager
    async def try_open_pit(
        self, *, index: str, keep_alive: str, **kwargs
    ) -> AsyncGenerator[Optional[PointInTime], None]:
        pit_id = None
        if await self.supports_pit:
            try:
                pit = await self.open_point_in_time(
                    index=index, keep_alive=keep_alive, **kwargs
                )
                yield pit
            finally:
                if pit_id is not None:
                    await self._close_pit(pit_id)
        else:
            yield None

    async def to_neo4j(
        self,
        index: str,
        bodies: List[Mapping[str, Any]],
        *,
        neo4j_import_worker_factory: Callable[[str], Neo4jImportWorker],
        num_neo4j_workers: int,
        concurrency: Optional[int] = None,
        max_records_in_memory: int,
        import_batch_size: int,
        imported_entity_label: str,
        progress: Optional[PercentProgress] = None,
    ) -> [int, List[LightCounters]]:
        if num_neo4j_workers <= 0:
            raise ValueError("num_neo4j_workers must be > 0")
        if concurrency is None:
            concurrency = self.max_concurrency
        logger.info(
            "Starting nodes import with %s neo4j and %s elasticsearch workers",
            num_neo4j_workers,
            concurrency,
        )
        queue_size = max(max_records_in_memory // import_batch_size, 1)
        if not queue_size:
            msg = (
                "import_batch_size must be < max_records_in_memory to leverage"
                " neo4j concurrency"
            )
            logger.warning(msg)
        queue = asyncio.Queue(maxsize=queue_size)

        # Start the consumer tasks
        neo4j_workers = [
            neo4j_import_worker_factory(f"neo4j-node-worker-{i}")
            for i in range(num_neo4j_workers)
        ]
        neo4j_tasks = [
            asyncio.create_task(worker(queue), name=worker.name)
            for worker in neo4j_workers
        ]
        # 1 slice is not supported...
        concurrency = max(concurrency, 2)
        with log_elapsed_time_cm(
            logger,
            logging.INFO,
            f"Retrieved all {imported_entity_label} from elasticsearch in"
            f" {{elapsed_time}} !",
        ):
            n_imported = await self._fill_import_queue(
                index=index,
                import_batch_size=import_batch_size,
                bodies=bodies,
                queue=queue,
                max_concurrency=concurrency,
                progress=progress,
            )
        # Wait for the queue to be fully consumed
        queue_complete = asyncio.create_task(queue.join())
        await asyncio.wait(
            [queue_complete, *neo4j_tasks], return_when=asyncio.FIRST_COMPLETED
        )
        if not queue_complete.done():
            logger.error("some task failed before queue was consumed...")
            for task in neo4j_tasks:
                if task.done():  # There was an exception
                    raise task.exception()
        # Cancel consumer tasks which will make them break their infinite loop and
        # return the result summaries
        for task in neo4j_tasks:
            task.cancel()
        # If there's nothing to import workers are cancelled right await, no need to
        # wait for tasks to end
        if not n_imported:
            return n_imported, []
        # Wait for all results to exit and return summaries
        summaries = await asyncio.gather(*neo4j_tasks)
        for i in range(len(neo4j_tasks)):  # pylint: disable=consider-using-enumerate
            del neo4j_tasks[i]
        summaries = sum(summaries, [])
        return n_imported, summaries

    async def write_concurrently_neo4j_csvs(
        self,
        index: str,
        query: Optional[Mapping[str, Any]],
        *,
        pit: PointInTime,
        nodes_f: Optional[TextIO],
        relationships_f: Optional[TextIO],
        nodes_header: Optional[List[str]],
        relationships_header: Optional[List[str]],
        to_neo4j_nodes: Optional[Callable[[Dict], List[Dict[str, str]]]],
        to_neo4j_relationships: Optional[Callable[[Dict], List[Dict[str, str]]]],
        concurrency: Optional[int] = None,
        keep_alive: Optional[str] = None,
    ) -> [Optional[int], Optional[int]]:
        if concurrency is None:
            concurrency = self.max_concurrency
        if keep_alive is None:
            keep_alive = self.keep_alive
        # 1 slice is not supported...
        concurrency = max(concurrency, 2)
        # Max should be at least 2
        bodies = (
            sliced_search(
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
            self._write_search_to_neo4j_csvs_with_lock(
                body=body,
                es_index=index,
                nodes_f=nodes_f,
                relationships_f=relationships_f,
                nodes_header=nodes_header,
                relationships_header=relationships_header,
                to_neo4j_nodes=to_neo4j_nodes,
                to_neo4j_relationships=to_neo4j_relationships,
                lock=lock,
            )
            for body in bodies
        )
        res = await asyncio.gather(*tasks)
        if not res:
            total_nodes = None if nodes_header is None else []
            total_rels = None if relationships_header is None else []
            return total_nodes, total_rels
        total_nodes, total_rels = res[0]
        for node_count, rel_count in res[1:]:
            if node_count is not None:
                total_nodes += node_count
            if rel_count is not None:
                total_rels += rel_count
        return total_nodes, total_rels

    async def _fill_import_queue(
        self,
        index: str,
        queue: asyncio.Queue,
        *,
        import_batch_size: int,
        bodies: List[Mapping],
        max_concurrency: Optional[int] = None,
        progress: Optional[PercentProgress],
    ) -> int:
        if not bodies:
            return 0
        lock = asyncio.Lock()
        buffer = []
        raw_progress = None
        if progress is not None and bodies:
            count_queries = [{QUERY: b[QUERY]} for b in bodies]
            count_tasks = (self.count(index=index, body=b) for b in count_queries)
            res_it = run_with_concurrency(count_tasks, max_concurrency=max_concurrency)
            ne_counts = [c[COUNT] async for c in res_it]
            ne_counts = sum(ne_counts)
            if not ne_counts:
                return 0
            raw_progress = to_raw_progress(progress, max_progress=ne_counts)
        gens = [
            self._fill_import_buffer(
                index=index,
                queue=queue,
                import_batch_size=import_batch_size,
                lock=lock,
                buffer=buffer,
                body=body,
            )
            for body in bodies
        ]
        if max_concurrency is None:
            max_concurrency = len(gens)
        imported = 0
        concurrent_streamer = iterate_with_concurrency(
            gens, max_concurrency=max_concurrency
        )
        progress_tasks = set()
        async with concurrent_streamer.stream() as streamer:
            async for n_imported in streamer:
                imported += n_imported
                if raw_progress is not None:
                    progress_task = asyncio.create_task(raw_progress(imported))
                    progress_tasks.add(progress_task)
                    progress_task.add_done_callback(progress_tasks.discard)
        if buffer:
            imported += await _enqueue_import_batch(queue, buffer)
            if progress is not None:
                progress_task = asyncio.create_task(raw_progress(imported))
                progress_tasks.add(progress_task)
                progress_task.add_done_callback(progress_tasks.discard)
        return imported

    async def _fill_import_buffer(
        self,
        *,
        index: str,
        queue: asyncio.Queue,
        import_batch_size: int,
        lock: asyncio.Lock,
        buffer: List[Dict],
        **kwargs,
    ) -> AsyncGenerator[int, None]:
        # Since we can't provide an async generator to the neo4j client, let's store
        # results into a list fitting in memory, which will then be split into
        # transaction batches
        async for res in self.poll_search_pages(index, **kwargs):
            async with lock:
                buffer.extend(res[HITS][HITS])
                if len(buffer) >= import_batch_size:
                    yield await _enqueue_import_batch(queue, buffer)

    async def _write_search_to_neo4j_csvs_with_lock(
        self,
        *,
        es_index: str,
        nodes_f: Optional[TextIO],
        relationships_f: Optional[TextIO],
        nodes_header: Optional[List[str]],
        relationships_header: Optional[List[str]],
        to_neo4j_nodes: Optional[Callable[[Dict], List[Dict[str, str]]]],
        to_neo4j_relationships: Optional[Callable[[Dict], List[Dict[str, str]]]],
        lock: asyncio.Lock,
        **kwargs,
    ) -> Tuple[Optional[int], Optional[int]]:
        total_nodes = None
        total_rels = None
        if bool(nodes_f) != bool(to_neo4j_nodes):
            msg = (
                "A function to extract nodes from ES records must be provided when a"
                " csv node file is provided and vice versa"
            )
            raise ValueError(msg)
        if bool(relationships_f) != bool(to_neo4j_relationships):
            msg = (
                "A function to extract relationships from ES must be provided when a"
                " csv relationships file is provided and vice versa"
            )
            raise ValueError(msg)
        if nodes_f is not None:
            total_nodes = 0
        if relationships_f is not None:
            total_rels = 0
        async for res in self.poll_search_pages(es_index, **kwargs):
            # Let's not lock the while converting the rows even if that implies using
            # some memory
            nodes_rows = None
            rels_rows = None
            if to_neo4j_nodes is not None:
                nodes_rows = [
                    row for hit in res[HITS][HITS] for row in to_neo4j_nodes(hit)
                ]
                total_nodes += len(nodes_rows)
            if to_neo4j_relationships is not None:
                rels_rows = [
                    row
                    for hit in res[HITS][HITS]
                    for row in to_neo4j_relationships(hit)
                ]
                total_rels += len(rels_rows)
            async with lock:
                if nodes_rows is not None:
                    write_neo4j_csv(
                        nodes_f,
                        rows=nodes_rows,
                        header=nodes_header,
                        write_header=False,
                    )
                    nodes_f.flush()
                if rels_rows is not None:
                    write_neo4j_csv(
                        relationships_f,
                        rows=rels_rows,
                        header=relationships_header,
                        write_header=False,
                    )
                    relationships_f.flush()
        return total_nodes, total_rels


class ESClient(ESClientABC, AsyncElasticsearch):
    min_pit_version = _ES_PIT_VERSION

    def __init__(
        self,
        *,
        pagination: int,
        keep_alive: str = "1m",
        max_concurrency: int = 5,
        max_retries: int = 0,
        max_retry_wait_s: Union[int, float] = 60,
        **kwargs,
    ):
        ESClientABC.__init__(
            self,
            pagination=pagination,
            keep_alive=keep_alive,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            max_retry_wait_s=max_retry_wait_s,
        )
        AsyncElasticsearch.__init__(self, **kwargs)

    async def search(self, body: Optional[Dict], index: Optional[str], **kwargs):
        # pylint: disable=unexpected-keyword-arg
        if SIZE in kwargs:
            msg = f"{ESClient.__name__} run searches using the pagination_size"
            raise ValueError(msg)
        return await super(ESClientABC, self).search(
            body=body, index=index, size=self.pagination_size, **kwargs
        )

    # TODO: this should be class attr
    @cached_property
    def _pit_id(self) -> str:
        return ID

    def default_sort(self, pit_search: bool) -> str:
        if pit_search:
            return f"{SHARD_DOC_}:{ASC}"
        return f"{DOC_}:{ASC}"

    async def _close_pit(self, pit_id: str):
        await self.close_point_in_time(body={ID: pit_id})


try:
    from opensearchpy import AsyncOpenSearch

    class OSClient(ESClientABC, AsyncOpenSearch):
        min_pit_version = _OS_PIT_VERSION

        def __init__(
            self,
            *,
            pagination: int,
            keep_alive: str = "1m",
            max_concurrency: int = 5,
            max_retries: int = 0,
            max_retry_wait_s: Union[int, float] = 60,
            **kwargs,
        ):
            ESClientABC.__init__(
                self,
                pagination=pagination,
                keep_alive=keep_alive,
                max_concurrency=max_concurrency,
                max_retries=max_retries,
                max_retry_wait_s=max_retry_wait_s,
            )
            AsyncOpenSearch.__init__(self, **kwargs)

        async def search(self, body: Optional[Dict], index: Optional[str], **kwargs):
            # pylint: disable=unexpected-keyword-arg
            if SIZE in kwargs:
                msg = f"{OSClient.__name__} run searches using the pagination_size"
                raise ValueError(msg)
            return await super(ESClientABC, self).search(
                body=body, index=index, size=self.pagination_size, **kwargs
            )

        # TODO: this should be class attr
        @cached_property
        def _pit_id(self) -> str:
            return "pid_id"

        def default_sort(
            self, pit_search: bool
        ) -> str:  # pylint: disable=unused-argument
            return f"{DOC_}:{ASC}"

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


async def _enqueue_import_batch(queue: asyncio.Queue, buffer: List[Dict]) -> int:
    # Let's monitor the time it takes to enqueue the batch, if it's long it
    # means that more neo4j workers are needed
    with log_elapsed_time_cm(
        logger, logging.DEBUG, "Waited {elapsed_time} to enqueue batch"
    ):
        queued = deepcopy(buffer)
        n_queued = len(queued)
        await queue.put(queued)
        # TODO: remove this cleanup is it's not causing the memory leak
        for item in buffer:
            del item
        buffer.clear()
        for item in queued:
            del item
        del queued
        return n_queued


def sliced_search(
    query: Optional[Dict[str, Any]],
    *,
    pit: Optional[PointInTime],
    id_: int,
    max_: int,
    keep_alive: Optional[str] = None,
) -> Dict:
    if query is None:
        query = {QUERY: match_all()}
    else:
        query = deepcopy(query)
    update = {SLICE: {ID: id_, MAX: max_}}
    if pit is not None:
        update[PIT] = pit
        if keep_alive is not None:
            update[PIT][KEEP_ALIVE] = keep_alive
    query.update(update)
    return query


class _retry_if_error_code(retry_base):  # pylint: disable=invalid-name
    def __init__(self, codes: Collection[int]):
        self._recoverable = set(codes)

    def __call__(self, retry_state: RetryCallState) -> bool:
        if retry_state.outcome.failed:
            exc = retry_state.outcome.exception()
            return (
                isinstance(exc, TransportError) and exc.status_code in self._recoverable
            )
        return False
