from unittest.mock import AsyncMock, patch

import pytest

from neo4j_app.core.neo4j.projects import create_project_registry_db
from neo4j_app.run.run import debug_app


@pytest.mark.asyncio
async def test_should_create_project_registry_db_with_enterprise_distribution():
    # Given
    app = debug_app()
    mocked_driver = AsyncMock()
    with patch("neo4j_app.core.AppConfig.to_neo4j_driver") as mocked_get_driver:
        # When
        mocked_get_driver.return_value = mocked_driver
        await create_project_registry_db(app)

    # Then
    mocked_driver.execute_query.assert_called_once_with(
        "CREATE DATABASE $registry_db IF NOT EXISTS",
        registry_db="datashare-project-registry",
    )


@pytest.mark.asyncio
async def test_projects_tx():
    assert False


@pytest.mark.asyncio
async def test_init_project():
    assert False


@pytest.mark.asyncio
async def test_init_project_should_raise_for_reserved_name():
    assert False


@pytest.mark.asyncio
async def test_init_project_should_be_idempotent():
    assert False
