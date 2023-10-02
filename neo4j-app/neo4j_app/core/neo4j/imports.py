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
        neo4j_db: str,
        import_fn: Neo4Import,
        *,
        transaction_batch_size: int,
        to_neo4j: Optional[Callable[[Any], Optional[Dict]]] = None,
    ):
        self._name = name
        self._neo4j_driver = neo4j_driver
        self._neo4j_db = neo4j_db
        self._import_fn = import_fn
        self._transaction_batch_size = transaction_batch_size
        self._to_neo4j = to_neo4j
        self._summaries = None

    async def __call__(self, queue: asyncio.Queue) -> List[neo4j.ResultSummary]:
        self._summaries = []
        try:
            while "Waiting forever until the task is cancelled":
                batch = await queue.get()
                if self._to_neo4j is not None:
                    batch = (row for rec in batch for row in self._to_neo4j(rec))
                    batch = [rec for rec in batch if rec is not None]
                logger.debug(
                    "worker %s importing %s records, (queuesize=%s)",
                    self.name,
                    len(batch),
                    queue.qsize(),
                )
                # TODO: execute this in background instead ?
                await self._import_batch(batch)
                queue.task_done()
                logger.debug(
                    "worker %s imported batch (queuesize=%s)", self.name, queue.qsize()
                )
        # Let's return
        except asyncio.CancelledError:
            logger.debug("worker %s received cancellation, exiting", self.name)
            return self._summaries

    async def _import_batch(self, batch: List[Dict]):
        async with self._neo4j_driver.session(database=self._neo4j_db) as neo4j_session:
            summary = await self._import_fn(
                neo4j_session,
                batch,
                transaction_batch_size=self._transaction_batch_size,
            )
        self._summaries.append(summary)

    @cached_property
    def name(self) -> str:
        return self._name
