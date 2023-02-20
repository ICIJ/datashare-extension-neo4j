# pylint: disable=redefined-outer-name
import asyncio
from pathlib import Path
from typing import AsyncGenerator, Dict, Generator

import neo4j
import pytest
import pytest_asyncio
from elasticsearch.helpers import async_streaming_bulk
from fastapi import APIRouter
from neo4j import AsyncGraphDatabase
from starlette.testclient import TestClient

from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.app.utils import create_app
from neo4j_app.core.utils.pydantic import BaseICIJModel

DATA_DIR = Path(__file__).parents[3].joinpath(".data")
NEO4J_TEST_IMPORT_DIR = DATA_DIR.joinpath("neo4j", "import")
NEO4J_TEST_PORT = 7687
_INDEX_BODY = {
    "mappings": {
        "properties": {"type": {"type": "keyword"}, "documentId": {"type": "keyword"}}
    }
}


# Define a session level even_loop fixture to overcome limitation explained here:
# https://github.com/tortoise/tortoise-orm/issues/638#issuecomment-830124562
@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_client_session() -> TestClient:
    config = AppConfig(
        neo4j_project="test-datashare-project",
        neo4j_import_dir=str(NEO4J_TEST_IMPORT_DIR),
        neo4j_app_host="127.0.0.1",
        neo4j_port=NEO4J_TEST_PORT,
        debug=True,
    )
    app = create_app(config)
    # Add a router which generates error in order to test error handling
    app.include_router(_error_router())
    with TestClient(app) as client:
        yield client


@pytest.fixture()
def test_client(
    test_client_session: TestClient,
    # Wipe ES by requiring the "function" level es client
    es_test_client: ESClient,
    # Same for neo4j
    neo4j_test_session: neo4j.AsyncSession,
) -> TestClient:
    # pylint: disable=unused-argument
    return test_client_session


def _error_router() -> APIRouter:
    class SomeExpectedBody(BaseICIJModel):
        mandatory_field: str

    error_router = APIRouter()

    @error_router.get("/internal-errors/generate")
    def _generate():
        raise ValueError("this is the internal error")

    @error_router.post("/internal-errors")
    async def _post_internal_error(body: SomeExpectedBody) -> SomeExpectedBody:
        return body

    return error_router


@pytest.fixture(scope="session")
def error_test_client_session(test_client_session: TestClient) -> TestClient:
    app = test_client_session.app
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


@pytest_asyncio.fixture()
async def es_test_client() -> AsyncGenerator[ESClient, None]:
    test_index = "test_index"
    es = ESClient(project_index=test_index, hosts=[{"host": "localhost", "port": 9200}])
    await es.indices.delete(index="_all")
    await es.indices.create(
        index=test_index,
        body=_INDEX_BODY,
    )
    yield es
    await es.close()


@pytest_asyncio.fixture(scope="session")
async def neo4j_test_driver_session() -> AsyncGenerator[neo4j.AsyncDriver, None]:
    uri = f"neo4j://127.0.0.1:{NEO4J_TEST_PORT}"
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
            "_id": f"doc-{i}",
            "_source": {
                "rootId": None,
                "dirname": f"dirname-{i}",
                "contentType": f"content-type-{i}",
                "contentLength": i**2,
                "extractionDate": "2023-02-06T13:48:22.3866",
                "path": f"dirname-{i}",
                "type": "Document",
            },
        }


def index_docs_ops(*, index_name: str, n: int) -> Generator[Dict, None, None]:
    for doc in make_docs(n):
        op = {
            "_op_type": "index",
            "_index": index_name,
        }
        op.update(doc)
        yield op


def index_noise_ops(*, index_name: str, n: int) -> Generator[Dict, None, None]:
    for i in range(n):
        op = {
            "_op_type": "index",
            "_index": index_name,
            "_id": f"noise-{i}",
            "_source": {"this": f"noise number {i}"},
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


async def index_noise(
    client: ESClient, *, index_name: str, n: int
) -> AsyncGenerator[Dict, None]:
    ops = index_noise_ops(index_name=index_name, n=n)
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
