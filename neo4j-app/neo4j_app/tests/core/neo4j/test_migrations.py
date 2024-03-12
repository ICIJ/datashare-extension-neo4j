import neo4j

from neo4j_app.core.neo4j.migrations import (
    migration_v_0_1_0_tx,
    migration_v_0_2_0_tx,
    migration_v_0_3_0_tx,
    migration_v_0_4_0_tx,
    migration_v_0_5_0_tx,
    migration_v_0_6_0,
    migration_v_0_7_0_tx,
    migration_v_0_8_0,
)


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


async def test_migration_v_0_5_0_tx(neo4j_test_session: neo4j.AsyncSession):
    # When
    await neo4j_test_session.execute_write(migration_v_0_5_0_tx)

    # Then
    indexes_res = await neo4j_test_session.run("SHOW INDEXES")
    existing_indexes = set()
    async for rec in indexes_res:
        existing_indexes.add(rec["name"])
    expected_indexes = [
        "index_named_entity_email_user",
        "index_named_entity_email_domain",
    ]
    for index in expected_indexes:
        assert index in expected_indexes


async def test_migration_v_0_6_0_tx(neo4j_test_session: neo4j.AsyncSession):
    # Given
    create_path = """CREATE (:NamedEntity)-[:APPEARS_IN {mentionIds: ['id-0', 'id-1']}
]->(:Document)"""
    await neo4j_test_session.run(create_path)
    # When
    await migration_v_0_6_0(neo4j_test_session)
    # Then
    match_path = "MATCH  (:NamedEntity)-[rel:APPEARS_IN]->(:Document) RETURN rel"
    res = await neo4j_test_session.run(match_path)
    res = await res.single(strict=True)
    rel = res["rel"]
    mention_counts = rel.get("mentionCount")
    assert mention_counts == 2


async def test_migration_v_0_7_0_tx(neo4j_test_session: neo4j.AsyncSession):
    # When
    await neo4j_test_session.execute_write(migration_v_0_7_0_tx)

    # Then
    indexes_res = await neo4j_test_session.run("SHOW INDEXES")
    existing_indexes = set()
    async for rec in indexes_res:
        existing_indexes.add(rec["name"])
    expected_indexes = [
        "index_document_created_at",
        "index_document_modified_at",
    ]
    for index in expected_indexes:
        assert index in expected_indexes


async def test_migration_v_0_8_0_tx(neo4j_test_session: neo4j.AsyncSession):
    # When
    await migration_v_0_8_0(neo4j_test_session)
    # Then
    count_query = "MATCH (s:_ProjectStatistics) RETURN count(*) AS nStats"
    res = await neo4j_test_session.run(count_query)
    count = await res.single()
    assert count["nStats"] == 1
    constraints_res = await neo4j_test_session.run("SHOW CONSTRAINTS")
    existing_constraints = set()
    async for rec in constraints_res:
        existing_constraints.add(rec["name"])
    assert "constraint_stats_unique_id" in existing_constraints
