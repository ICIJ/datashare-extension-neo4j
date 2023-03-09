import neo4j

from neo4j_app.constants import MIGRATION_NODE, MIGRATION_VERSION


async def create_migration_unique_constraint_tx(tx: neo4j.AsyncTransaction):
    constraint_query = f"""CREATE CONSTRAINT constraint_migration_unique_version
IF NOT EXISTS 
FOR (m:{MIGRATION_NODE})
REQUIRE (m.{MIGRATION_VERSION}) IS UNIQUE
"""
    await tx.run(constraint_query)
