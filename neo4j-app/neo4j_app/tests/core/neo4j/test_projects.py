from unittest.mock import AsyncMock

import neo4j
import pytest

from neo4j_app.constants import PROJECT_REGISTRY_DB
from neo4j_app.core.neo4j import V_0_1_0
from neo4j_app.core.neo4j.migrations.migrate import (
    Neo4jMigration,
    init_project,
    retrieve_projects,
)
from neo4j_app.core.neo4j.projects import Project, create_project_registry_db
from neo4j_app.tests.conftest import fail_if_exception, mock_enterprise_


@pytest.mark.asyncio
async def test_should_create_project_registry_db_with_enterprise_distribution(
    mock_enterprise,
):
    # Given
    mocked_driver = AsyncMock()

    # When
    await create_project_registry_db(mocked_driver)

    # Then
    mocked_driver.execute_query.assert_called_once_with(
        "CREATE DATABASE $registry_db IF NOT EXISTS",
        registry_db="datashare-project-registry",
    )


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

    # When
    await init_project(neo4j_driver, project_name, registry, timeout_s=1, throttle_s=1)

    # Then
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
async def test_init_project_should_be_idempotent(neo4j_test_driver: neo4j.AsyncDriver):
    # Given
    neo4j_driver = neo4j_test_driver
    project_name = "test-project"
    registry = [V_0_1_0]
    await init_project(neo4j_driver, project_name, registry, timeout_s=1, throttle_s=1)

    # When
    with fail_if_exception("init_project is not idempotent"):
        await init_project(
            neo4j_driver, project_name, registry, timeout_s=1, throttle_s=1
        )

    # Then
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
