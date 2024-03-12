import logging
from copy import deepcopy
from typing import AsyncGenerator, Dict, Optional

import neo4j
from icij_common.neo4j.projects import project_db

from neo4j_app.constants import (
    DOC_NODE,
    DOC_PATH,
    EMAIL_RECEIVED_TYPE,
    EMAIL_SENT_TYPE,
    NE_APPEARS_IN_DOC,
    NE_MENTION_COUNT,
    NE_NODE,
)
from neo4j_app.core.objects import DumpFormat, ProjectStatistics

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


async def project_statistics(
    neo4j_driver: neo4j.AsyncDriver, project: str
) -> ProjectStatistics:
    neo4j_db = await project_db(neo4j_driver, project)
    async with neo4j_driver.session(database=neo4j_db) as sess:
        stats = await sess.execute_read(ProjectStatistics.from_neo4j)
    return stats


async def refresh_project_statistics(
    neo4j_driver: neo4j.AsyncDriver, project: str
) -> ProjectStatistics:
    neo4j_db = await project_db(neo4j_driver, project)
    async with neo4j_driver.session(database=neo4j_db) as sess:
        stats = await sess.execute_write(refresh_project_statistics_tx)
    return stats


async def _count_documents_tx(
    tx: neo4j.AsyncTransaction, document_counts_key="nDocs"
) -> int:
    doc_query = f"""
    MATCH (doc:{DOC_NODE}) RETURN count(*) as nDocs
    """
    doc_res = await tx.run(doc_query)
    doc_res = await doc_res.single()
    n_docs = doc_res[document_counts_key]
    return n_docs


async def _count_entities_tx(
    tx: neo4j.AsyncTransaction,
    entity_labels_key: str = "neLabels",
    entity_counts_key: str = "nMentions",
) -> Dict[str, int]:
    entity_query = f"""MATCH (ne:{NE_NODE})
WITH ne, labels(ne) as neLabels
MATCH (ne)-[rel:{NE_APPEARS_IN_DOC}]->()
RETURN neLabels, sum(rel.{NE_MENTION_COUNT}) as nMentions"""
    entity_res = await tx.run(entity_query)
    n_ents = dict()
    async for rec in entity_res:
        labels = [l for l in rec[entity_labels_key] if l != NE_NODE]
        if len(labels) != 1:
            msg = (
                "Expected named entity to have exactly 2 labels."
                " Refactor this function."
            )
            raise ValueError(msg)
        n_ents[labels[0]] = rec[entity_counts_key]
    return n_ents


async def refresh_project_statistics_tx(
    tx: neo4j.AsyncTransaction,
) -> ProjectStatistics:
    # We could update the stats directly in DB, however since _count_entities_tx needs
    # to perform advanced error handling, we quickly get back to Python before
    # re-writing the whole stats
    n_docs = await _count_documents_tx(tx)
    n_ents = await _count_entities_tx(tx)
    stats = await ProjectStatistics.to_neo4j_tx(tx, n_docs, n_ents)
    return stats
