import neo4j
import pytest

from neo4j_app.core.neo4j import create_document_and_ne_id_unique_constraint_tx


@pytest.mark.asyncio
async def test_create_document_and_ne_id_unique_constraint_tx(
    neo4j_test_session: neo4j.AsyncSession,
):
    # Given
    constraints_res = await neo4j_test_session.run("SHOW CONSTRAINTS")
    existing_constraints = set()
    async for rec in constraints_res:
        existing_constraints.add(rec["name"])

    # When
    await neo4j_test_session.execute_write(
        create_document_and_ne_id_unique_constraint_tx
    )

    # Then
    constraints_res = await neo4j_test_session.run("SHOW CONSTRAINTS")
    async for rec in constraints_res:
        existing_constraints.add(rec["name"])
    assert "constraint_document_unique_id" in existing_constraints
    assert "constraint_named_entity_unique_id" in existing_constraints
