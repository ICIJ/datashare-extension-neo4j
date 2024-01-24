import logging
from copy import deepcopy
from typing import AsyncGenerator, Optional

import neo4j

from neo4j_app.constants import (
    DOC_NODE,
    DOC_PATH,
    EMAIL_RECEIVED_TYPE,
    EMAIL_SENT_TYPE,
    NE_APPEARS_IN_DOC,
    NE_MENTION_COUNT,
    NE_NODE,
)
from neo4j_app.core.neo4j.projects import project_db
from neo4j_app.core.objects import DumpFormat, GraphCounts

logger = logging.getLogger(__name__)

_EXPORT_BATCH_SIZE = "batchSize"
_GRAPHML_DUMP_CONFIG = {
    "format": "gephi",
    "stream": True,
    "streamStatements": True,
    "readLabels": False,
    "storeNodeIds": False,
}

_CYPHER_DUMP_CONFIG = {
    "stream": True,
    "streamStatements": True,
    "writeNodeProperties": True,
    "format": "cypher-shell",
    "cypherFormat": "create",
    "useOptimizations": {"type": "UNWIND_BATCH", "unwindBatchSize": 1000},
}


def _make_default_query(default_docs_limit: Optional[int] = None) -> str:
    query = f"""MATCH (doc:{DOC_NODE})
WITH doc
ORDER BY doc.{DOC_PATH} ASC"""
    if isinstance(default_docs_limit, int):
        query += f"""
LIMIT {default_docs_limit}"""
    query += f"""
OPTIONAL MATCH (doc)-[\
rel:{NE_APPEARS_IN_DOC}|{EMAIL_SENT_TYPE}|{EMAIL_RECEIVED_TYPE}]-(ne:{NE_NODE})
RETURN apoc.coll.toSet(collect(doc) + collect(ne) + collect(rel)) AS values"""
    return query


async def dump_graph(
    project: str,
    dump_format: DumpFormat,
    neo4j_driver: neo4j.AsyncDriver,
    *,
    query: Optional[str] = None,
    default_docs_limit: Optional[int] = None,
    parallel: bool = None,
    export_batch_size: int,
) -> AsyncGenerator[str, None]:
    # TODO: support batchsize ?
    neo4j_db = await project_db(neo4j_driver, project)
    if query is None:
        query = _make_default_query(default_docs_limit)
    if dump_format is DumpFormat.GRAPHML:
        gen = _dump_subgraph_to_graphml(
            neo4j_driver,
            neo4j_db=neo4j_db,
            query=query,
            parallel=parallel,
            export_batch_size=export_batch_size,
        )
    elif dump_format is DumpFormat.CYPHER_SHELL:
        gen = _dump_subgraph_to_cypher(
            neo4j_driver,
            neo4j_db=neo4j_db,
            query=query,
            parallel=parallel,
            export_batch_size=export_batch_size,
        )
    else:
        raise ValueError(f'dump not supported for "{dump_format}" format')
    async for record in gen:
        yield record


async def _dump_subgraph_to_graphml(
    neo4j_driver: neo4j.AsyncDriver,
    *,
    neo4j_db: str,
    query: str,
    parallel: bool,
    export_batch_size: int,
) -> AsyncGenerator[str, None]:
    runtime = "CYPHER runtime=parallel" if parallel else ""
    config = deepcopy(_GRAPHML_DUMP_CONFIG)
    config[_EXPORT_BATCH_SIZE] = export_batch_size
    async with neo4j_driver.session(database=neo4j_db) as sess:
        neo4j_query = f"""{runtime}
CALL apoc.export.graphml.query($query_filter, null, $config) YIELD data
RETURN data;
"""
        logger.debug("executing dump query: %s", query)
        res = await sess.run(
            neo4j_query, config=_GRAPHML_DUMP_CONFIG, query_filter=query
        )
        async for rec in res:
            yield rec["data"]


async def _dump_subgraph_to_cypher(
    neo4j_driver: neo4j.AsyncDriver,
    *,
    neo4j_db: str,
    query: str,
    parallel: bool,
    export_batch_size: int,
) -> AsyncGenerator[str, None]:
    runtime = "CYPHER runtime=parallel" if parallel else ""
    async with neo4j_driver.session(database=neo4j_db) as sess:
        neo4j_query = f"""{runtime}
CALL apoc.export.cypher.query($query_filter, null, $config) YIELD cypherStatements
RETURN cypherStatements;
"""
        config = deepcopy(_CYPHER_DUMP_CONFIG)
        config[_EXPORT_BATCH_SIZE] = export_batch_size
        logger.debug("executing dump query: %s", query)
        res = await sess.run(
            neo4j_query, config=_CYPHER_DUMP_CONFIG, query_filter=query
        )
        async for rec in res:
            yield rec["cypherStatements"]


async def count_documents_and_named_entities(
    neo4j_driver: neo4j.AsyncDriver, project: str, parallel: bool
) -> GraphCounts:
    neo4j_db = await project_db(neo4j_driver, project)
    async with neo4j_driver.session(database=neo4j_db) as sess:
        count = await sess.execute_read(
            _count_documents_and_named_entities_tx, parallel=parallel
        )
        return count


async def _count_documents_and_named_entities_tx(
    tx: neo4j.AsyncTransaction, parallel: bool
) -> GraphCounts:
    runtime = "CYPHER runtime=parallel" if parallel else ""
    doc_query = f"""{runtime}
MATCH (doc:{DOC_NODE}) RETURN count(*) as nDocs
"""
    doc_res = await tx.run(doc_query)
    entity_query = f"""{runtime}
MATCH (ne:{NE_NODE})
WITH DISTINCT labels(ne) as neLabels, ne
MATCH (ne)-[rel:{NE_APPEARS_IN_DOC}]->()
RETURN neLabels, sum(rel.{NE_MENTION_COUNT}) as nMentions"""
    entity_res = await tx.run(entity_query)
    count = await GraphCounts.from_neo4j(doc_res=doc_res, entity_res=entity_res)
    return count
