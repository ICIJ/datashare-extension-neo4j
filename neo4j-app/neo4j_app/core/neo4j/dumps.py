from typing import AsyncGenerator, Optional

import neo4j

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


async def dump_graph(
    dump_format: DumpFormat,
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_db: str,
    query: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    # TODO: support batchsize ?
    if dump_format is DumpFormat.GRAPHML:
        if query is None:
            gen = _dump_full_graph_to_graphml(neo4j_driver, neo4j_db)
        else:
            gen = _dump_subgraph_to_graphml(neo4j_driver, neo4j_db, query)
    elif dump_format is DumpFormat.CYPHER_SHELL:
        if query is None:
            gen = _dump_full_graph_to_cypher(neo4j_driver, neo4j_db)
        else:
            gen = _dump_subgraph_to_cypher(neo4j_driver, neo4j_db, query)
    else:
        raise ValueError(f'dump not supported for "{dump_format}" format')
    async for record in gen:
        yield record


async def _dump_full_graph_to_graphml(
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_db: str,
) -> AsyncGenerator[str, None]:
    async with neo4j_driver.session(database=neo4j_db) as sess:
        query = """CALL apoc.export.graphml.all(null, $config)
YIELD data
RETURN data;
"""
        res = await sess.run(query, config=_GRAPHML_DUMP_CONFIG)
        async for rec in res:
            yield rec["data"]


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


async def _dump_full_graph_to_cypher(
    neo4j_driver, neo4j_db
) -> AsyncGenerator[str, None]:
    async with neo4j_driver.session(database=neo4j_db) as sess:
        query = """CALL apoc.export.cypher.all(null, $config)
YIELD cypherStatements
RETURN cypherStatements;
"""
        res = await sess.run(query, config=_CYPHER_DUMP_CONFIG)
        async for rec in res:
            yield rec["cypherStatements"]


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
