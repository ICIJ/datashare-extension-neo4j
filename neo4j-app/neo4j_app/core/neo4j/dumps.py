from typing import AsyncGenerator, Optional

import neo4j

from neo4j_app.constants import MIGRATION_NODE
from neo4j_app.core.objects import DumpFormat

_GRAPHML_DUMP_CONFIG = {
    "format": "gephi",
    "batchSize": 20000,
    "stream": True,
    "readLabels": True,
    "storeNodeIds": True,
}

_CYPHER_DUMP_CONFIG = {
    "format": "cypher-shell",
    "cypherFormat": "create",
    "streamStatements": True,
    "batchSize": 20000,
    "useOptimizations": {"type": "UNWIND_BATCH", "unwindBatchSize": 100},
}

_DEFAULT_DUMP_QUERY = f"""MATCH (node)
OPTIONAL MATCH (d)-[r]-(other)
WHERE NOT any(l IN labels(node) WHERE l = '{MIGRATION_NODE}')
    AND NOT any(l IN labels(other) WHERE l = '{MIGRATION_NODE}')
RETURN d, r, other
"""


async def dump_graph(
    dump_format: DumpFormat,
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_db: str,
    query: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    # TODO: support batchsize ?
    if query is None:
        query = _DEFAULT_DUMP_QUERY
    if dump_format is DumpFormat.GRAPHML:
        gen = _dump_subgraph_to_graphml(neo4j_driver, neo4j_db, query)
    elif dump_format is DumpFormat.CYPHER_SHELL:
        gen = _dump_subgraph_to_cypher(neo4j_driver, neo4j_db, query)
    else:
        raise ValueError(f'dump not supported for "{dump_format}" format')
    async for record in gen:
        yield record


async def _dump_subgraph_to_graphml(
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_db: str,
    query: str,
) -> AsyncGenerator[str, None]:
    async with neo4j_driver.session(database=neo4j_db) as sess:
        neo4j_query = """CALL apoc.export.graphml.query($query_filter, null, $config)
YIELD data
RETURN data;
"""
        res = await sess.run(
            neo4j_query, config=_GRAPHML_DUMP_CONFIG, query_filter=query
        )
        async for rec in res:
            yield rec["data"]


async def _dump_subgraph_to_cypher(
    neo4j_driver, neo4j_db, query
) -> AsyncGenerator[str, None]:
    async with neo4j_driver.session(database=neo4j_db) as sess:
        neo4j_query = """CALL apoc.export.cypher.query($query_filter, null, $config)
YIELD cypherStatements
RETURN cypherStatements;
"""
        res = await sess.run(
            neo4j_query, config=_CYPHER_DUMP_CONFIG, query_filter=query
        )
        async for rec in res:
            yield rec["cypherStatements"]
