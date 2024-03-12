# pylint: disable=redefined-outer-name
import contextlib
from typing import AsyncGenerator

import neo4j
import pytest
from neo4j import AsyncGraphDatabase

import icij_common
from icij_common.neo4j.projects import NEO4J_COMMUNITY_DB

NEO4J_TEST_PORT = 7688
NEO4J_TEST_USER = "neo4j"
NEO4J_TEST_PASSWORD = "theneo4jpassword"
NEO4J_TEST_AUTH = neo4j.basic_auth(NEO4J_TEST_USER, NEO4J_TEST_PASSWORD)


async def wipe_db(session: neo4j.AsyncSession):
    # Indices and constraints
    query = "CALL apoc.schema.assert({}, {})"
    await session.run(query)
    # Documents
    query = """MATCH (n)
DETACH DELETE n
    """
    await session.run(query)


@contextlib.asynccontextmanager
async def _build_neo4j_driver():
    uri = f"neo4j://127.0.0.1:{NEO4J_TEST_PORT}"
    async with AsyncGraphDatabase.driver(  # pylint: disable=not-async-context-manager
        uri, auth=NEO4J_TEST_AUTH
    ) as driver:
        yield driver


@pytest.fixture(scope="module")
async def neo4j_test_driver_module() -> AsyncGenerator[neo4j.AsyncDriver, None]:
    async with _build_neo4j_driver() as driver:
        async with driver.session(database=neo4j.DEFAULT_DATABASE) as sess:
            await wipe_db(sess)
        yield driver


@pytest.fixture(scope="session")
async def neo4j_test_driver_session() -> AsyncGenerator[neo4j.AsyncDriver, None]:
    async with _build_neo4j_driver() as driver:
        async with driver.session(database=neo4j.DEFAULT_DATABASE) as sess:
            await wipe_db(sess)
        yield driver


@pytest.fixture()
async def neo4j_test_driver() -> AsyncGenerator[neo4j.AsyncDriver, None]:
    async with _build_neo4j_driver() as driver:
        async with driver.session(database="neo4j") as sess:
            await wipe_db(sess)
        yield driver


@pytest.fixture(scope="session")
async def neo4j_test_session_session(
    neo4j_test_driver_session: neo4j.Driver,
) -> AsyncGenerator[neo4j.AsyncSession, None]:
    driver = neo4j_test_driver_session
    async with driver.session(database="neo4j") as sess:
        await wipe_db(sess)
        yield sess


@pytest.fixture(scope="module")
async def neo4j_test_session_module(
    neo4j_test_session_session: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    session = neo4j_test_session_session
    await wipe_db(session)
    return session


@pytest.fixture()
async def neo4j_test_session(
    neo4j_test_session_session: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    session = neo4j_test_session_session
    await wipe_db(session)
    return session


async def mocked_is_enterprise(_: neo4j.AsyncDriver) -> bool:
    return True


@contextlib.asynccontextmanager
async def _mocked_project_db_session(
    neo4j_driver: neo4j.AsyncDriver, project: str  # pylint: disable=unused-argument
) -> neo4j.AsyncSession:
    async with neo4j_driver.session(database=NEO4J_COMMUNITY_DB) as sess:
        yield sess


async def _mocked_project_registry_db(
    neo4j_driver: neo4j.AsyncDriver,  # pylint: disable=unused-argument
) -> str:
    return NEO4J_COMMUNITY_DB


@pytest.fixture()
def mock_enterprise(monkeypatch):
    mock_enterprise_(monkeypatch)


def mock_enterprise_(monkeypatch):
    monkeypatch.setattr(
        icij_common.neo4j.projects, "project_db_session", _mocked_project_db_session
    )
    monkeypatch.setattr(
        icij_common.neo4j.migrate,
        "project_db_session",
        _mocked_project_db_session,
    )
    monkeypatch.setattr(
        icij_common.neo4j.projects,
        "project_registry_db",
        _mocked_project_registry_db,
    )
    monkeypatch.setattr(
        icij_common.neo4j.projects, "is_enterprise", mocked_is_enterprise
    )
