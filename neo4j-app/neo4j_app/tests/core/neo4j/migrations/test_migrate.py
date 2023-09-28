import logging
from datetime import datetime
from typing import List, Set

import neo4j
import pytest
import pytest_asyncio
from neo4j.exceptions import ClientError

import neo4j_app
from neo4j_app.constants import PROJECT_REGISTRY_DB
from neo4j_app.core.neo4j import Migration, V_0_1_0, migrate_db_schemas
from neo4j_app.core.neo4j.migrations import migrate
from neo4j_app.core.neo4j.migrations.migrate import (
    MigrationError,
    MigrationStatus,
    Neo4jMigration,
    init_project,
    retrieve_projects,
)
from neo4j_app.core.neo4j.projects import Project
from neo4j_app.tests.conftest import (
    TEST_PROJECT,
    fail_if_exception,
    mock_enterprise_,
    mocked_is_enterprise,
    wipe_db,
)

_BASE_REGISTRY = [V_0_1_0]


@pytest_asyncio.fixture(scope="function")
async def _migration_index_and_constraint(
    neo4j_test_driver: neo4j.AsyncDriver,
) -> neo4j.AsyncDriver:
    await init_project(
        neo4j_test_driver, TEST_PROJECT, _BASE_REGISTRY, timeout_s=30, throttle_s=0.1
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
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
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
            "MATCH (m:_Migration) RETURN m as migration"
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
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
):
    # Given
    neo4j_driver = _migration_index_and_constraint
    registry = [_MIGRATION_0]

    # When
    query = """CREATE (:_Migration {
    version: $version,
    project: $project,
    label: $label,
    started: $started 
 })"""

    await neo4j_driver.execute_query(
        query,
        version=str(_MIGRATION_0.version),
        project=TEST_PROJECT,
        label=_MIGRATION_0.label,
        started=datetime.now(),
    )
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
        sess: neo4j.AsyncSession, project: str  # pylint: disable=unused-argument
    ) -> List[Neo4jMigration]:
        return [
            Neo4jMigration(
                project=TEST_PROJECT,
                version="0.1.0",
                label="migration in progress",
                status=MigrationStatus.IN_PROGRESS,
                started=datetime.now(),
            )
        ]

    monkeypatch.setattr(migrate, "project_migrations_tx", mocked_get_migrations)

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
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
):
    # Given
    neo4j_driver = _migration_index_and_constraint
    caplog.set_level(logging.INFO, logger=neo4j_app.__name__)

    async def mocked_get_migrations(
        sess: neo4j.AsyncSession, project: str  # pylint: disable=unused-argument
    ) -> List[Neo4jMigration]:
        return []

    # No migration in progress
    monkeypatch.setattr(migrate, "project_migrations_tx", mocked_get_migrations)

    # However we simulate _MIGRATION_0 being running just before our migrate_db_schema
    # by inserting it in progress
    query = """CREATE (m:_Migration {
    project: $project,
    version: $version, 
    label: 'someLabel', 
    started: $started
})
"""
    await neo4j_driver.execute_query(
        query,
        project="test_project",
        version=str(_MIGRATION_0.version),
        label=str(_MIGRATION_0.label),
        started=datetime.now(),
        status=MigrationStatus.IN_PROGRESS.value,
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


@pytest.mark.asyncio
@pytest.mark.parametrize("enterprise", [True, False])
async def test_retrieve_project_dbs(
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
    enterprise: bool,
    monkeypatch,
):
    # Given
    neo4j_driver = _migration_index_and_constraint

    if enterprise:
        mock_enterprise_(monkeypatch)

    projects = await retrieve_projects(neo4j_driver)

    # Then
    assert projects == [Project(name=TEST_PROJECT)]


@pytest.mark.asyncio
async def test_migrate_should_use_registry_db_when_with_enterprise_support(
    _migration_index_and_constraint: neo4j.AsyncDriver,  # pylint: disable=invalid-name
    monkeypatch,
):
    # Given
    registry = _BASE_REGISTRY

    monkeypatch.setattr(
        neo4j_app.core.neo4j.projects, "is_enterprise", mocked_is_enterprise
    )
    neo4j_driver = _migration_index_and_constraint

    # When/Then
    expected = (
        "Unable to get a routing table for database 'datashare-project-registry'"
        " because this database does not exist"
    )
    with pytest.raises(ClientError, match=expected):
        await migrate_db_schemas(neo4j_driver, registry, timeout_s=10, throttle_s=0.1)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_enterprise", [True, False])
async def test_init_project(
    neo4j_test_driver: neo4j.AsyncDriver, is_enterprise: bool, monkeypatch
):
    # Given
    neo4j_driver = neo4j_test_driver
    project_name = "test-project"
    registry = [V_0_1_0]

    if is_enterprise:
        mock_enterprise_(monkeypatch)
        with pytest.raises(ClientError) as ctx:
            await init_project(
                neo4j_driver, project_name, registry, timeout_s=1, throttle_s=1
            )
        expected_code = "Neo.ClientError.Statement.UnsupportedAdministrationCommand"
        assert ctx.value.code == expected_code
    else:
        # When
        existed = await init_project(
            neo4j_driver, project_name, registry, timeout_s=1, throttle_s=1
        )
        assert not existed

        # Then
        projects = await retrieve_projects(neo4j_driver)
        assert projects == [Project(name=project_name)]
        db_migrations_recs, _, _ = await neo4j_driver.execute_query(
            "MATCH (m:_Migration) RETURN m as migration"
        )
        db_migrations = [
            Neo4jMigration.from_neo4j(rec, key="migration")
            for rec in db_migrations_recs
        ]
        assert len(db_migrations) == 1
        migration = db_migrations[0]
        assert migration.version == V_0_1_0.version


@pytest.mark.asyncio
async def test_init_project_should_be_idempotent(neo4j_test_driver: neo4j.AsyncDriver):
    # Given
    neo4j_driver = neo4j_test_driver
    project_name = "test-project"
    registry = [V_0_1_0]
    await init_project(neo4j_driver, project_name, registry, timeout_s=1, throttle_s=1)

    # When
    with fail_if_exception("init_project is not idempotent"):
        existed = await init_project(
            neo4j_driver, project_name, registry, timeout_s=1, throttle_s=1
        )

    # Then
    assert existed

    projects = await retrieve_projects(neo4j_driver)
    assert projects == [Project(name=project_name)]
    db_migrations_recs, _, _ = await neo4j_driver.execute_query(
        "MATCH (m:_Migration) RETURN m as migration"
    )
    db_migrations = [
        Neo4jMigration.from_neo4j(rec, key="migration") for rec in db_migrations_recs
    ]
    assert len(db_migrations) == 1
    migration = db_migrations[0]
    assert migration.version == V_0_1_0.version


@pytest.mark.asyncio
async def test_init_project_should_raise_for_reserved_name(
    neo4j_test_driver_session: neo4j.AsyncDriver,
):
    # Given
    neo4j_driver = neo4j_test_driver_session
    project_name = PROJECT_REGISTRY_DB

    # When/then
    expected = (
        'Bad luck, name "datashare-project-registry" is reserved for'
        " internal use. Can't initialize project"
    )
    with pytest.raises(ValueError, match=expected):
        await init_project(
            neo4j_driver, project_name, registry=[], timeout_s=1, throttle_s=1
        )
