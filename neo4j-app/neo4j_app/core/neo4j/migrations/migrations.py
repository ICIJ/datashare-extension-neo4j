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
)


async def migration_v_0_1_0_tx(tx: neo4j.AsyncTransaction):
    await _create_project_unique_name_constraint_tx(tx)
    await _create_migration_unique_project_and_version_constraint_tx(tx)


async def migration_v_0_2_0_tx(tx: neo4j.AsyncTransaction):
    await _create_document_and_ne_id_unique_constraint_tx(tx)
    await _create_ne_mention_norm_index_tx(tx)


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
