import neo4j

from neo4j_app.constants import (
    DOC_ID,
    DOC_NODE,
    MIGRATION_NODE,
    MIGRATION_PROJECT,
    MIGRATION_VERSION,
    NE_ID,
    NE_MENTION_NORM,
    NE_NODE,
    PROJECT_NAME,
    PROJECT_NODE,
    TASK_CREATED_AT,
    TASK_ERROR_ID,
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_AT,
    TASK_ID,
    TASK_NODE,
    TASK_TYPE,
)


async def migration_v_0_1_0_tx(tx: neo4j.AsyncTransaction):
    await _create_project_unique_name_constraint_tx(tx)
    await _create_migration_unique_project_and_version_constraint_tx(tx)


async def migration_v_0_2_0_tx(tx: neo4j.AsyncTransaction):
    await _create_document_and_ne_id_unique_constraint_tx(tx)
    await _create_ne_mention_norm_index_tx(tx)


async def migration_v_0_3_0_tx(tx: neo4j.AsyncTransaction):
    await _create_task_index_and_constraints(tx)


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
