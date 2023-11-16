import neo4j
import pytest

from neo4j_app.core.neo4j.migrations.migrations import (
    migration_v_0_1_0_tx,
    migration_v_0_2_0_tx,
    migration_v_0_3_0_tx,
    migration_v_0_4_0_tx,
)


@pytest.mark.asyncio
async def test_migration_v_0_1_0_tx(
    neo4j_test_session: neo4j.AsyncSession,
):
    # When
    await neo4j_test_session.execute_write(migration_v_0_1_0_tx)

    # Then
    constraints_res = await neo4j_test_session.run("SHOW CONSTRAINTS")
    existing_constraints = set()
    async for rec in constraints_res:
        existing_constraints.add(rec["name"])
    assert "constraint_migration_unique_project_and_version" in existing_constraints


@pytest.mark.asyncio
async def test_migration_v_0_2_0_tx(
    neo4j_test_session: neo4j.AsyncSession,
):
    # When
    await neo4j_test_session.execute_write(migration_v_0_2_0_tx)

    # Then
    indexes_res = await neo4j_test_session.run("SHOW INDEXES")
    existing_indexes = set()
    async for rec in indexes_res:
        existing_indexes.add(rec["name"])
    assert "index_ne_mention_norm" in existing_indexes
    constraints_res = await neo4j_test_session.run("SHOW CONSTRAINTS")
    existing_constraints = set()
    async for rec in constraints_res:
        existing_constraints.add(rec["name"])
    assert "constraint_named_entity_unique_id" in existing_constraints
    assert "constraint_document_unique_id" in existing_constraints


@pytest.mark.asyncio
async def test_migration_v_0_3_0_tx(neo4j_test_session: neo4j.AsyncSession):
    # When
    await neo4j_test_session.execute_write(migration_v_0_3_0_tx)

    # Then
    indexes_res = await neo4j_test_session.run("SHOW INDEXES")
    existing_indexes = set()
    async for rec in indexes_res:
        existing_indexes.add(rec["name"])
    expected_indexes = [
        "index_task_status",
        "index_task_created_at",
        "index_task_type",
        "index_task_error_timestamp",
    ]
    for index in expected_indexes:
        assert index in expected_indexes
    constraints_res = await neo4j_test_session.run("SHOW CONSTRAINTS")
    existing_constraints = set()
    async for rec in constraints_res:
        existing_constraints.add(rec["name"])
    assert "constraint_task_unique_id" in existing_constraints


@pytest.mark.asyncio
async def test_migration_v_0_4_0_tx(neo4j_test_session: neo4j.AsyncSession):
    # When
    await neo4j_test_session.execute_write(migration_v_0_4_0_tx)

    # Then
    indexes_res = await neo4j_test_session.run("SHOW INDEXES")
    existing_indexes = set()
    async for rec in indexes_res:
        existing_indexes.add(rec["name"])
    expected_indexes = [
        "index_document_path",
        "index_document_content_type",
    ]
    for index in expected_indexes:
        assert index in expected_indexes
