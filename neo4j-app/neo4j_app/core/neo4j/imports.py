import asyncio
import logging
from functools import cached_property
from typing import Any, Callable, Dict, List, Optional, Protocol

import neo4j

logger = logging.getLogger(__name__)


class Neo4Import(Protocol):
    async def __call__(
        self,
        neo4j_session: neo4j.AsyncSession,
        records: List[Dict],
        *,
        transaction_batch_size: int,
    ) -> neo4j.ResultSummary:
        ...


class Neo4jImportWorker:
    def __init__(
        self,
        name: str,
        neo4j_driver: neo4j.AsyncDriver,
        import_fn: Neo4Import,
        *,
        transaction_batch_size: int,
        to_neo4j: Optional[Callable[[Any], Optional[Dict]]] = None,
    ):
        self._name = name
        self._neo4j_driver = neo4j_driver
        self._import_fn = import_fn
        self._transaction_batch_size = transaction_batch_size
        self._to_neo4j = to_neo4j

    async def __call__(self, queue: asyncio.Queue) -> List[neo4j.ResultSummary]:
        # TODO:
        #  - use https://github.com/jd/tenacity to implement retry with backoff in
        #  case of network error
        #  - after several failure, requeue the job...
        summaries = []
        try:
            while "Waiting forever until the task is cancelled":
                import_batch = await queue.get()
                if self._to_neo4j is not None:
                    import_batch = (
                        row for rec in import_batch for row in self._to_neo4j(rec)
                    )
                    import_batch = [rec for rec in import_batch if rec is not None]
                logger.debug(
                    "Worker %s is starting import of %s records",
                    self.name,
                    len(import_batch),
                )
                async with self._neo4j_driver.session() as neo4j_session:
                    summary = await self._import_fn(
                        neo4j_session,
                        import_batch,
                        transaction_batch_size=self._transaction_batch_size,
                    )
                logger.debug("Worker %s completed import !", self.name)
                summaries.append(summary)
                queue.task_done()
        # Let's return
        except asyncio.CancelledError:
            logger.debug(
                "Worker %s received cancellation signal, returning results", self.name
            )
            return summaries

    @cached_property
    def name(self) -> str:
        return self._name
