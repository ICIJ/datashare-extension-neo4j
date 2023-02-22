# pylint: disable=redefined-outer-name
import asyncio
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Generator, Tuple

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

# TODO: at a high level it's a waste to have to repeat code for each fixture level,
#  let's try to find a way to define the scope dynamically:
#  https://docs.pytest.org/en/6.2.x/fixture.html#dynamic-scope

DATA_DIR = Path(__file__).parents[3].joinpath(".data")
NEO4J_TEST_IMPORT_DIR = DATA_DIR.joinpath("neo4j", "import")
NEO4J_TEST_PORT = 7687
_INDEX_BODY = {
    "mappings": {
        "properties": {
            "type": {"type": "keyword"},
            "documentId": {"type": "keyword"},
            "join": {"type": "join", "relations": {"Document": "NamedEntity"}},
        }
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
def test_client_session(
    # Require ES to create indices and wipe ES
    es_test_client_session: ESClient,
) -> TestClient:
    # pylint: disable=unused-argument
    config = AppConfig(
        debug=True,
        es_default_page_size=5,
        neo4j_project="test-datashare-project",
        neo4j_import_dir=str(NEO4J_TEST_IMPORT_DIR),
        neo4j_app_host="127.0.0.1",
        neo4j_port=NEO4J_TEST_PORT,
    )
    app = create_app(config)
    # Add a router which generates error in order to test error handling
    app.include_router(_error_router())
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="module")
def test_client_module(
    test_client_session: TestClient,
    # Wipe ES by requiring the "module" level es client
    es_test_client_module: ESClient,
    # Same for neo4j
    neo4j_test_session_module: neo4j.AsyncSession,
) -> TestClient:
    # pylint: disable=unused-argument
    return test_client_session


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


def _make_test_client() -> ESClient:
    test_index = "test-datashare-project"
    es = ESClient(
        project_index=test_index,
        hosts=[{"host": "localhost", "port": 9200}],
        pagination=3,
    )
    return es


@pytest_asyncio.fixture(scope="session")
async def es_test_client_session() -> AsyncGenerator[ESClient, None]:
    es = _make_test_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index=es.project_index, body=_INDEX_BODY)
    yield es
    await es.close()


@pytest_asyncio.fixture(scope="module")
async def es_test_client_module() -> AsyncGenerator[ESClient, None]:
    es = _make_test_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index=es.project_index, body=_INDEX_BODY)
    yield es
    await es.close()


@pytest_asyncio.fixture()
async def es_test_client() -> AsyncGenerator[ESClient, None]:
    es = _make_test_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index=es.project_index, body=_INDEX_BODY)
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


@pytest_asyncio.fixture(scope="module")
async def neo4j_test_session_module(
    neo4j_test_session_session: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    session = neo4j_test_session_session
    await session.execute_write(_wipe_db_tx)
    return session


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
                "join": {"name": "Document"},
            },
        }


def make_named_entities(n: int) -> Generator[Dict, None, None]:
    for i in range(n):
        yield {
            "_id": f"named-entity-{i}",
            "_source": {
                "join": {"name": "NamedEntity", "parent": f"doc-{i}"},
                "type": "NamedEntity",
                "offsets": list(range(1, max(i, 1, 2))),
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
            "_source": {"someAttribute": f"noise number {i} attribute"},
        }
        yield op


def index_named_entities_ops(*, index_name: str, n: int) -> Generator[Dict, None, None]:
    for ent in make_named_entities(n):
        op = {
            "_op_type": "index",
            "_index": index_name,
            "_routing": "DocumentNamedEntityRoute",
        }
        op.update(ent)
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


async def index_named_entities(
    client: ESClient, *, index_name: str, n: int
) -> AsyncGenerator[Dict, None]:
    ops = index_named_entities_ops(index_name=index_name, n=n)
    # Let's wait to make this operation visible to the search
    refresh = "wait_for"
    async for res in async_streaming_bulk(client, actions=ops, refresh=refresh):
        yield res


# TODO: make the num_docs_in_neo4j configurable to that it can be called dynamically
@pytest_asyncio.fixture(scope="module")
async def insert_docs_in_neo4j_module(
    neo4j_test_session_module: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    neo4j_session = neo4j_test_session_module
    query, docs = await _make_create_docs_query()
    await neo4j_session.run(query, docs)
    return neo4j_session


@pytest_asyncio.fixture(scope="function")
async def insert_docs_in_neo4j(
    neo4j_test_session: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    neo4j_session = neo4j_test_session
    query, docs = await _make_create_docs_query()
    await neo4j_session.run(query, docs)
    return neo4j_session


async def _make_create_docs_query() -> Tuple[str, Dict[str, Any]]:
    num_docs_in_neo4j = 10  # Should be < to the number of docs in ES
    docs = {"docs": [{"id": f"doc-{i}"} for i in range(num_docs_in_neo4j)]}
    query = """UNWIND $docs as docProps
CREATE (n:Document {})
SET n = docProps
"""
    return query, docs


@pytest_asyncio.fixture(scope="function")
async def wipe_named_entities(
    neo4j_test_session_module: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    neo4j_session = neo4j_test_session_module
    query = """MATCH (ent:NamedEntity)
DETACH DELETE ent
"""
    await neo4j_session.run(query)
    return neo4j_session


async def _wipe_db_tx(tx: neo4j.AsyncTransaction):
    # Indices and constraints
    query = "CALL apoc.schema.assert({}, {})"
    await tx.run(query)
    # Documents
    query = """MATCH (n)
DETACH DELETE n
    """
    await tx.run(query)
