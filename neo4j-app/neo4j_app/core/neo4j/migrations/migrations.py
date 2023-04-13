import neo4j

from neo4j_app.constants import (
    DOC_ID,
    DOC_NODE,
    MIGRATION_NODE,
    MIGRATION_VERSION,
    NE_ID,
    NE_MENTION_NORM,
    NE_NODE,
)


async def create_migration_unique_constraint_tx(tx: neo4j.AsyncTransaction):
    constraint_query = f"""CREATE CONSTRAINT constraint_migration_unique_version
IF NOT EXISTS 
FOR (m:{MIGRATION_NODE})
REQUIRE (m.{MIGRATION_VERSION}) IS UNIQUE
"""
    await tx.run(constraint_query)


async def create_document_and_ne_id_unique_constraint_tx(tx: neo4j.AsyncTransaction):
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


async def replace_ne_constraint_on_id_by_index_on_mention_norm_tx(
    tx: neo4j.AsyncTransaction,
):
    delete_constraint_on_id = """DROP CONSTRAINT constraint_named_entity_unique_id
IF EXISTS"""
    await tx.run(delete_constraint_on_id)
    create_constraint_on_mention_norm = f"""
CREATE INDEX index_ne_mention_norm IF NOT EXISTS
FOR (ent:{NE_NODE})
ON (ent.{NE_MENTION_NORM})
"""
    await tx.run(create_constraint_on_mention_norm)
