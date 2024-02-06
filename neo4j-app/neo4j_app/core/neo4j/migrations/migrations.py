import logging

import neo4j

from neo4j_app.constants import (
    DOC_CONTENT_TYPE,
    DOC_CREATED_AT,
    DOC_ID,
    DOC_MODIFIED_AT,
    DOC_NODE,
    DOC_PATH,
    EMAIL_DOMAIN,
    EMAIL_USER,
    MIGRATION_NODE,
    MIGRATION_PROJECT,
    MIGRATION_VERSION,
    NE_APPEARS_IN_DOC,
    NE_ID,
    NE_IDS,
    NE_MENTION_COUNT,
    NE_MENTION_NORM,
    NE_NODE,
    PROJECT_NAME,
    PROJECT_NODE,
    STATS_ID,
    STATS_NODE,
    TASK_CREATED_AT,
    TASK_ERROR_ID,
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_AT,
    TASK_ID,
    TASK_LOCK_NODE,
    TASK_LOCK_TASK_ID,
    TASK_LOCK_WORKER_ID,
    TASK_NODE,
    TASK_TYPE,
)
from neo4j_app.core.neo4j.graphs import refresh_project_statistics_tx

logger = logging.getLogger(__name__)


async def migration_v_0_1_0_tx(tx: neo4j.AsyncTransaction):
    await _create_project_unique_name_constraint_tx(tx)
    await _create_migration_unique_project_and_version_constraint_tx(tx)


async def migration_v_0_2_0_tx(tx: neo4j.AsyncTransaction):
    await _create_document_and_ne_id_unique_constraint_tx(tx)
    await _create_ne_mention_norm_index_tx(tx)


async def migration_v_0_3_0_tx(tx: neo4j.AsyncTransaction):
    await _create_task_index_and_constraints(tx)


async def migration_v_0_4_0_tx(tx: neo4j.AsyncTransaction):
    await _create_document_path_and_content_type_indexes(tx)


async def migration_v_0_5_0_tx(tx: neo4j.AsyncTransaction):
    await _create_email_user_and_domain_indexes(tx)


async def migration_v_0_6_0(sess: neo4j.AsyncSession):
    query = f"""MATCH (:{NE_NODE})-[rel:{NE_APPEARS_IN_DOC}]->(:{DOC_NODE})
    CALL {{
        WITH rel
        SET rel.{NE_MENTION_COUNT} = size(rel.{NE_IDS})
    }} IN TRANSACTIONS OF 10000 ROWS"""
    await sess.run(query)


async def migration_v_0_7_0_tx(tx: neo4j.AsyncTransaction):
    await _create_document_created_and_modified_at_indexes(tx)


async def migration_v_0_8_0(sess: neo4j.AsyncSession):
    await sess.execute_write(_create_project_stats_unique_constraint_tx)
    await sess.execute_write(refresh_project_statistics_if_needed_tx)


async def _create_document_and_ne_id_unique_constraint_tx(tx: neo4j.AsyncTransaction):
    doc_query = f"""CREATE CONSTRAINT constraint_document_unique_id
IF NOT EXISTS 
FOR (doc:{DOC_NODE})
REQUIRE (doc.{DOC_ID}) IS UNIQUE
"""
    await tx.run(doc_query)
    ne_query = f"""CREATE CONSTRAINT constraint_named_entity_unique_id
IF NOT EXISTS 
FOR (ent:{NE_NODE})
REQUIRE (ent.{NE_ID}) IS UNIQUE
"""
    await tx.run(ne_query)


async def _create_ne_mention_norm_index_tx(
    tx: neo4j.AsyncTransaction,
):
    create_index_on_mention_norm = f"""
CREATE INDEX index_ne_mention_norm IF NOT EXISTS
FOR (ent:{NE_NODE})
ON (ent.{NE_MENTION_NORM})
"""
    await tx.run(create_index_on_mention_norm)


async def _create_project_unique_name_constraint_tx(tx: neo4j.AsyncTransaction):
    constraint_query = f"""CREATE CONSTRAINT constraint_project_unique_name
IF NOT EXISTS
FOR (p:{PROJECT_NODE})
REQUIRE (p.{PROJECT_NAME}) IS UNIQUE
"""
    await tx.run(constraint_query)


async def _create_migration_unique_project_and_version_constraint_tx(
    tx: neo4j.AsyncTransaction,
):
    constraint_query = f"""CREATE CONSTRAINT
     constraint_migration_unique_project_and_version
IF NOT EXISTS 
FOR (m:{MIGRATION_NODE})
REQUIRE (m.{MIGRATION_VERSION}, m.{MIGRATION_PROJECT}) IS UNIQUE
"""
    await tx.run(constraint_query)


async def _create_task_index_and_constraints(tx: neo4j.AsyncTransaction):
    constraint_query = f"""CREATE CONSTRAINT constraint_task_unique_id
IF NOT EXISTS 
FOR (task:{TASK_NODE})
REQUIRE (task.{TASK_ID}) IS UNIQUE"""
    await tx.run(constraint_query)
    created_at_query = f"""CREATE INDEX index_task_created_at IF NOT EXISTS
FOR (task:{TASK_NODE})
ON (task.{TASK_CREATED_AT})"""
    await tx.run(created_at_query)
    type_query = f"""CREATE INDEX index_task_type IF NOT EXISTS
FOR (task:{TASK_NODE})
ON (task.{TASK_TYPE})"""
    await tx.run(type_query)
    error_timestamp_query = f"""CREATE INDEX index_task_error_timestamp IF NOT EXISTS
FOR (task:{TASK_ERROR_NODE})
ON (task.{TASK_ERROR_OCCURRED_AT})"""
    await tx.run(error_timestamp_query)
    error_id_query = f"""CREATE CONSTRAINT constraint_task_error_unique_id IF NOT EXISTS
FOR (task:{TASK_ERROR_NODE})
REQUIRE (task.{TASK_ERROR_ID}) IS UNIQUE"""
    await tx.run(error_id_query)
    task_lock_task_id_query = f"""CREATE CONSTRAINT constraint_task_lock_unique_task_id
IF NOT EXISTS
FOR (lock:{TASK_LOCK_NODE})
REQUIRE (lock.{TASK_LOCK_TASK_ID}) IS UNIQUE"""
    await tx.run(task_lock_task_id_query)
    task_lock_worker_id_query = f"""CREATE INDEX index_task_lock_worker_id IF NOT EXISTS
FOR (lock:{TASK_LOCK_NODE})
ON (lock.{TASK_LOCK_WORKER_ID})"""
    await tx.run(task_lock_worker_id_query)


async def _create_document_path_and_content_type_indexes(tx: neo4j.AsyncTransaction):
    doc_path_index = f"""CREATE INDEX index_document_path IF NOT EXISTS
FOR (doc:{DOC_NODE})
ON (doc.{DOC_PATH})"""
    await tx.run(doc_path_index)
    doc_content_type_index = f"""CREATE INDEX index_document_content_type IF NOT EXISTS
FOR (doc:{DOC_NODE})
ON (doc.{DOC_CONTENT_TYPE})"""
    await tx.run(doc_content_type_index)


async def _create_email_user_and_domain_indexes(tx: neo4j.AsyncTransaction):
    ne_email_user_index = f"""CREATE INDEX index_named_entity_email_user IF NOT EXISTS
FOR (ne:{NE_NODE})
ON (ne.{EMAIL_USER})"""
    await tx.run(ne_email_user_index)
    ne_email_domain_index = f"""
CREATE INDEX index_named_entity_email_domain IF NOT EXISTS
FOR (ne:{NE_NODE})
ON (ne.{EMAIL_DOMAIN})"""
    await tx.run(ne_email_domain_index)


async def _add_mention_count_to_named_entity_relationship(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (:{NE_NODE})-[rel:{NE_APPEARS_IN_DOC}]->(:{DOC_NODE})
CALL {{
    WITH rel
    SET rel.{NE_MENTION_COUNT} = size(rel.{NE_IDS})
}} IN TRANSACTIONS OF 10000 ROWS"""
    await tx.run(query)


async def _create_document_created_and_modified_at_indexes(tx: neo4j.AsyncTransaction):
    created_at_index = f"""CREATE INDEX index_document_created_at IF NOT EXISTS
FOR (doc:{DOC_NODE})
ON (doc.{DOC_CREATED_AT})"""
    await tx.run(created_at_index)
    modified_at_index = f"""CREATE INDEX index_document_modified_at IF NOT EXISTS
FOR (doc:{DOC_NODE})
ON (doc.{DOC_MODIFIED_AT})"""
    await tx.run(modified_at_index)


async def _create_project_stats_unique_constraint_tx(tx: neo4j.AsyncTransaction):
    stats_query = f"""CREATE CONSTRAINT constraint_stats_unique_id
IF NOT EXISTS 
FOR (s:{STATS_NODE})
REQUIRE (s.{STATS_ID}) IS UNIQUE
"""
    await tx.run(stats_query)


async def refresh_project_statistics_if_needed_tx(tx: neo4j.AsyncTransaction):
    count_query = f"MATCH (s:{STATS_NODE}) RETURN s"
    res = await tx.run(count_query)
    counts = await res.single()
    if counts is None:
        logger.info("missing graph statistics, computing them...")
        await refresh_project_statistics_tx(tx)
    else:
        logger.info("stats are already computed skipping !")
