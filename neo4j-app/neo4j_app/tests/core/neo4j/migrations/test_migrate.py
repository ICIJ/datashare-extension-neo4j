import logging
from datetime import datetime
from typing import List, Set

import neo4j
import pytest
import pytest_asyncio

import neo4j_app
from neo4j_app.core.neo4j import FIRST_MIGRATION, Migration, migrate_db_schemas
from neo4j_app.core.neo4j.migrations import migrate
from neo4j_app.core.neo4j.migrations.migrate import (
    MigrationError,
    MigrationStatus,
    Neo4jMigration,
)
from neo4j_app.tests.conftest import wipe_db

_BASE_REGISTRY = [FIRST_MIGRATION]


@pytest_asyncio.fixture(scope="function")
async def _migration_index_and_constraint(
    neo4j_test_driver: neo4j.AsyncDriver,
) -> neo4j.AsyncDriver:
    await migrate_db_schemas(
        neo4j_test_driver, _BASE_REGISTRY, timeout_s=30, throttle_s=0.1
    )
    return neo4j_test_driver


async def _create_indexes_tx(tx: neo4j.AsyncTransaction):
    index_query_0 = "CREATE INDEX index0 IF NOT EXISTS FOR (n:Node) ON (n.attribute0)"
    await tx.run(index_query_0)
    index_query_1 = "CREATE INDEX index1 IF NOT EXISTS FOR (n:Node) ON (n.attribute1)"
    await tx.run(index_query_1)


async def _drop_constraint_tx(tx: neo4j.AsyncTransaction):
    drop_index_query = "DROP INDEX index0 IF EXISTS"
    await tx.run(drop_index_query)


# noinspection PyTypeChecker
_MIGRATION_0 = Migration(
    version="0.2.0",
    label="create index and constraint",
    migration_fn=_create_indexes_tx,
)
# noinspection PyTypeChecker
_MIGRATION_1 = Migration(
    version="0.3.0",
    label="drop constraint",
    migration_fn=_drop_constraint_tx,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "registry,expected_indexes,not_expected_indexes",
    [
        # No migration
        ([], set(), set()),
        # Single
        ([_MIGRATION_0], {"index0", "index1"}, set()),
        # Multiple ordered
        ([_MIGRATION_0, _MIGRATION_1], {"index1"}, {"index0"}),
        # Multiple unordered
        ([_MIGRATION_1, _MIGRATION_0], {"index1"}, {"index0"}),
    ],
)
async def test_migrate_db_schema(
    _migration_index_and_constraint: neo4j.AsyncDriver,  # pylint: disable=invalid-name
    registry: List[Migration],
    expected_indexes: Set[str],
    not_expected_indexes: Set[str],
):
    # Given
    neo4j_driver = _migration_index_and_constraint

    # When
    await migrate_db_schemas(neo4j_driver, registry, timeout_s=10, throttle_s=0.1)

    # Then
    index_res, _, _ = await neo4j_driver.execute_query("SHOW INDEXES")
    existing_indexes = set()
    for rec in index_res:
        existing_indexes.add(rec["name"])
    missing_indexes = expected_indexes - existing_indexes
    assert not missing_indexes
    assert not not_expected_indexes.intersection(existing_indexes)

    if registry:
        db_migrations_recs, _, _ = await neo4j_driver.execute_query(
            "MATCH (m:Migration) RETURN m as migration"
        )
        db_migrations = [
            Neo4jMigration.from_neo4j(rec, key="migration")
            for rec in db_migrations_recs
        ]
        assert len(db_migrations) == len(registry) + 1
        assert all(m.status is MigrationStatus.DONE for m in db_migrations)
        max_version = max(m.version for m in registry)
        db_version = max(m.version for m in db_migrations)
        assert db_version == max_version


@pytest.mark.asyncio
async def test_migrate_db_schema_should_raise_after_timeout(
    neo4j_test_driver_session: neo4j.AsyncDriver,
):
    # Given
    neo4j_driver = neo4j_test_driver_session
    registry = [_MIGRATION_0]

    # When
    expected_msg = "Migration timeout expired"
    with pytest.raises(MigrationError, match=expected_msg):
        await migrate_db_schemas(neo4j_driver, registry, timeout_s=0, throttle_s=0.1)


@pytest.mark.asyncio
async def test_migrate_db_schema_should_wait_when_other_migration_in_progress(
    caplog,
    monkeypatch,
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
):
    # Given
    neo4j_driver_0 = _migration_index_and_constraint
    caplog.set_level(logging.INFO, logger=neo4j_app.__name__)

    async def mocked_get_migrations(
        sess: neo4j.AsyncSession,  # pylint: disable=unused-argument
    ) -> List[Neo4jMigration]:
        return [
            Neo4jMigration(
                version="0.1.0",
                label="migration in progress",
                status=MigrationStatus.IN_PROGRESS,
                started=datetime.now(),
            )
        ]

    monkeypatch.setattr(migrate, "migrations_tx", mocked_get_migrations)

    # When/Then
    expected_msg = "Migration timeout expired "
    with pytest.raises(MigrationError, match=expected_msg):
        timeout_s = 0.5
        wait_s = 0.1
        await migrate_db_schemas(
            neo4j_driver_0,
            [_MIGRATION_0, _MIGRATION_1],
            timeout_s=timeout_s,
            throttle_s=wait_s,
        )
    # Check that we've slept at least once otherwise timeout must be increased...
    assert any(
        rec.name == "neo4j_app.core.neo4j.migrations.migrate"
        and f"waiting for {wait_s}" in rec.message
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_migrate_db_schema_should_wait_when_other_migration_just_started(
    monkeypatch,
    caplog,
    _migration_index_and_constraint: neo4j.AsyncDriver,  # pylint: disable=invalid-name
):
    # Given
    neo4j_driver = _migration_index_and_constraint
    caplog.set_level(logging.INFO, logger=neo4j_app.__name__)

    async def mocked_get_migrations(
        sess: neo4j.AsyncSession,  # pylint: disable=unused-argument
    ) -> List[Neo4jMigration]:
        return []

    # No migration in progress
    monkeypatch.setattr(migrate, "migrations_tx", mocked_get_migrations)

    # However we simulate _MIGRATION_0 being running just before our migrate_db_schema
    # by inserting it in progress
    query = """CREATE (m:Migration {
    version: $version, 
    label: 'someLabel', 
    started: $started
})
"""
    await neo4j_driver.execute_query(
        query, version=str(_MIGRATION_0.version), started=datetime.now()
    )
    try:
        # When/Then
        expected_msg = "Migration timeout expired "
        with pytest.raises(MigrationError, match=expected_msg):
            timeout_s = 0.5
            wait_s = 0.1
            await migrate_db_schemas(
                neo4j_driver,
                [_MIGRATION_0],
                timeout_s=timeout_s,
                throttle_s=wait_s,
            )
        # Check that we've slept at least once otherwise timeout must be increased...
        assert any(
            rec.name == "neo4j_app.core.neo4j.migrations.migrate"
            and "just started" in rec.message
            for rec in caplog.records
        )
    finally:
        # Don't forget to cleanup other the DB will be locked
        async with neo4j_driver.session(database="neo4j") as sess:
            await wipe_db(sess)
