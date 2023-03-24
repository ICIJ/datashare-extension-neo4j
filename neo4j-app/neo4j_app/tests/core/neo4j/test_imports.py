import asyncio
from typing import Dict, List

import neo4j
import pytest

from neo4j_app.core.neo4j import Neo4jImportWorker


async def _dummy_import(
    neo4j_session: neo4j.AsyncSession,
    records: List[Dict],
    *,
    transaction_batch_size: int,
) -> neo4j.ResultSummary:
    query = """
UNWIND $rows as row
CALL {
    WITH row
    MERGE (node:DummyNode {nodeId: row.id})
    SET node.nodeId = row.id
} IN TRANSACTIONS OF $batchSize ROWS
"""
    res = await neo4j_session.run(query, rows=records, batchSize=transaction_batch_size)
    summary = await res.consume()
    return summary


@pytest.mark.asyncio
@pytest.mark.parametrize("num_workers", [1, 2])
async def test_neo4_import_worker(
    num_workers: int, neo4j_test_driver: neo4j.AsyncDriver
):
    # Given
    neo4j_driver = neo4j_test_driver
    transaction_batch_size = 2
    num_records = 22
    import_batch_size = 3
    records = [{"id": f"id-{i}"} for i in range(num_records)]
    workers = [
        Neo4jImportWorker(
            f"worker-{i}",
            neo4j_driver=neo4j_driver,
            import_fn=_dummy_import,
            transaction_batch_size=transaction_batch_size,
        )
        for i in range(num_workers)
    ]
    queue = asyncio.Queue()
    for start in range(0, num_records, import_batch_size):
        queue.put_nowait(records[start : start + import_batch_size])

    # When
    worker_tasks = [asyncio.create_task(worker(queue)) for worker in workers]
    # Wait for queue to be processed
    await queue.join()
    for task in worker_tasks:
        # Cancel tasks
        task.cancel()
    # Wait for tasks be collect results
    summaries = await asyncio.gather(*worker_tasks)
    summaries = sum(summaries, [])

    # Then
    num_created = sum(summary.counters.nodes_created for summary in summaries)
    assert num_created == num_records
