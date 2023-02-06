# pylint: disable=redefined-outer-name
import asyncio
from pathlib import Path
from typing import AsyncGenerator, Dict, Generator

import neo4j
import pytest
import pytest_asyncio
from elasticsearch.helpers import async_streaming_bulk
from neo4j import AsyncGraphDatabase
from starlette.testclient import TestClient

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.run.utils import create_app

DATA_DIR = Path(__file__).parents[3] / ".data"


# Define a session level even_loop fixture to overcome limitation explained here:
# https://github.com/tortoise/tortoise-orm/issues/638#issuecomment-830124562
@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_client() -> TestClient:
    app = create_app()
    with TestClient(app) as client:
        yield client


@pytest_asyncio.fixture()
async def es_test_client() -> AsyncGenerator[ESClient, None]:
    test_index = "test_index"
    es = ESClient(project_index=test_index, hosts=[{"host": "localhost", "port": 9200}])
    await es.indices.delete(index="_all")
    await es.indices.create(index=test_index)
    yield es
    await es.close()


@pytest_asyncio.fixture(scope="session")
async def neo4j_test_driver_session() -> AsyncGenerator[neo4j.AsyncDriver, None]:
    uri = "neo4j://127.0.0.1:7687"
    async with AsyncGraphDatabase.driver(  # pylint: disable=not-async-context-manager
        uri, auth=None
    ) as driver:
        yield driver


@pytest_asyncio.fixture(scope="session")
async def neo4j_test_session_session(
    neo4j_test_driver_session: neo4j.Driver,
) -> AsyncGenerator[neo4j.AsyncSession, None]:
    driver = neo4j_test_driver_session
    async with driver.session(database=neo4j.DEFAULT_DATABASE) as sess:
        yield sess


@pytest_asyncio.fixture()
async def neo4j_test_session(
    neo4j_test_session_session: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    session = neo4j_test_session_session
    await session.execute_write(_wipe_db_tx)
    return session


def make_docs(n: int) -> Generator[Dict, None, None]:
    for i in range(n):
        yield {
            "documentId": f"document-{i}",
            "rootId": None,
            "dirname": f"dirname-{i}",
            "contentType": f"content-type-{i}",
            "contentLength": i**2,
            "extractionDate": "2023-02-06T13:48:22.3866",
            "path": f"dirname-{i}",
        }


def index_docs_ops(*, index_name: str, n: int) -> Generator[Dict, None, None]:
    for i, doc in enumerate(make_docs(n)):
        op = {
            "_op_type": "index",
            "_index": index_name,
            "_id": f"doc-{i}",
            "doc": doc,
        }
        yield op


async def index_docs(
    client: ESClient, *, index_name: str, n: int
) -> AsyncGenerator[Dict, None]:
    ops = index_docs_ops(index_name=index_name, n=n)
    # Let's wait to make this operation visible to the search
    refresh = "wait_for"
    async for res in async_streaming_bulk(client, actions=ops, refresh=refresh):
        yield res


async def _wipe_db_tx(tx: neo4j.AsyncTransaction):
    # Indices and constraints
    query = "CALL apoc.schema.assert({}, {})"
    await tx.run(query)
    # Documents
    query = """MATCH (n)
DETACH DELETE n
    """
    await tx.run(query)
