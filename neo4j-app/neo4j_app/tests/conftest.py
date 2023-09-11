# pylint: disable=redefined-outer-name
import abc
import asyncio
import contextlib
import os
import random
import traceback
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Generator, Optional, Tuple, Union

import neo4j
import pytest
import pytest_asyncio
from elasticsearch.helpers import async_streaming_bulk
from fastapi import APIRouter
from neo4j import AsyncGraphDatabase
from starlette.testclient import TestClient

from neo4j_app.app.utils import create_app
from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClient, ESClientABC
from neo4j_app.core.elasticsearch.client import PointInTime
from neo4j_app.core.utils.pydantic import BaseICIJModel


# TODO: at a high level it's a waste to have to repeat code for each fixture level,
#  let's try to find a way to define the scope dynamically:
#  https://docs.pytest.org/en/6.2.x/fixture.html#dynamic-scope

DATA_DIR = Path(__file__).parents[3].joinpath(".data")
NEO4J_TEST_IMPORT_DIR = DATA_DIR.joinpath("neo4j", "import")
NEO4J_IMPORT_PREFIX = Path(os.sep).joinpath(".neo4j", "import")
ELASTICSEARCH_TEST_PORT = 9201
NEO4J_TEST_PORT = 7688
NEO4J_TEST_USER = "neo4j"
NEO4J_TEST_PASSWORD = "theneo4jpassword"
NEO4J_TEST_AUTH = neo4j.basic_auth(NEO4J_TEST_USER, NEO4J_TEST_PASSWORD)

_INDEX_BODY = {
    "mappings": {
        "properties": {
            "type": {"type": "keyword"},
            "documentId": {"type": "keyword"},
            "join": {"type": "join", "relations": {"Document": "NamedEntity"}},
        }
    }
}
TEST_INDEX = "test-datashare-project"


class MockedESClient(ESClientABC, metaclass=abc.ABCMeta):
    async def search(self, **kwargs) -> Dict[str, Any]:
        # pylint: disable=arguments-differ
        return await self._mocked_search(**kwargs)

    async def scroll(self, **kwargs) -> Any:
        # pylint: disable=arguments-differ
        return await self._mocked_search(**kwargs)

    async def clear_scroll(self, **kwargs) -> Any:
        # pylint: disable=arguments-differ
        pass

    @contextlib.asynccontextmanager
    async def try_open_pit(
        self, *, index: str, keep_alive: str, **kwargs
    ) -> AsyncGenerator[Optional[PointInTime], None]:
        yield dict()

    @abc.abstractmethod
    async def _mocked_search(self, **kwargs):
        pass


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
        elasticsearch_address=f"http://127.0.0.1:{ELASTICSEARCH_TEST_PORT}",
        es_default_page_size=5,
        neo4j_project="test-datashare-project",
        neo4j_app_host="127.0.0.1",
        neo4j_port=NEO4J_TEST_PORT,
        neo4j_user=NEO4J_TEST_USER,
        neo4j_password=NEO4J_TEST_PASSWORD,
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
    es = ESClient(
        hosts=[{"host": "localhost", "port": ELASTICSEARCH_TEST_PORT}],
        pagination=3,
    )
    return es


@pytest_asyncio.fixture(scope="session")
async def es_test_client_session() -> AsyncGenerator[ESClient, None]:
    es = _make_test_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index="test-datashare-project", body=_INDEX_BODY)
    yield es
    await es.close()


@pytest_asyncio.fixture(scope="module")
async def es_test_client_module() -> AsyncGenerator[ESClient, None]:
    es = _make_test_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index="test-datashare-project", body=_INDEX_BODY)
    yield es
    await es.close()


@pytest_asyncio.fixture()
async def es_test_client() -> AsyncGenerator[ESClient, None]:
    es = _make_test_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index="test-datashare-project", body=_INDEX_BODY)
    yield es
    await es.close()


@contextlib.asynccontextmanager
async def _build_neo4j_driver():
    uri = f"neo4j://127.0.0.1:{NEO4J_TEST_PORT}"
    async with AsyncGraphDatabase.driver(  # pylint: disable=not-async-context-manager
        uri, auth=NEO4J_TEST_AUTH
    ) as driver:
        yield driver


@pytest_asyncio.fixture(scope="module")
async def neo4j_test_driver_module() -> AsyncGenerator[neo4j.AsyncDriver, None]:
    async with _build_neo4j_driver() as driver:
        async with driver.session(database=neo4j.DEFAULT_DATABASE) as sess:
            await wipe_db(sess)
        yield driver


@pytest_asyncio.fixture(scope="session")
async def neo4j_test_driver_session() -> AsyncGenerator[neo4j.AsyncDriver, None]:
    async with _build_neo4j_driver() as driver:
        async with driver.session(database=neo4j.DEFAULT_DATABASE) as sess:
            await wipe_db(sess)
        yield driver


@pytest_asyncio.fixture()
async def neo4j_test_driver() -> AsyncGenerator[neo4j.AsyncDriver, None]:
    async with _build_neo4j_driver() as driver:
        async with driver.session(database="neo4j") as sess:
            await wipe_db(sess)
        yield driver


@pytest_asyncio.fixture(scope="session")
async def neo4j_test_session_session(
    neo4j_test_driver_session: neo4j.Driver,
) -> AsyncGenerator[neo4j.AsyncSession, None]:
    driver = neo4j_test_driver_session
    async with driver.session(database="neo4j") as sess:
        await wipe_db(sess)
        yield sess


@pytest_asyncio.fixture(scope="module")
async def neo4j_test_session_module(
    neo4j_test_session_session: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    session = neo4j_test_session_session
    await wipe_db(session)
    return session


@pytest_asyncio.fixture()
async def neo4j_test_session(
    neo4j_test_session_session: neo4j.AsyncSession,
) -> neo4j.AsyncSession:
    session = neo4j_test_session_session
    await wipe_db(session)
    return session


def make_docs(n: int) -> Generator[Dict, None, None]:
    random.seed(a=777)
    for i in random.sample(list(range(n)), k=n):
        yield {
            "_id": f"doc-{i}",
            "_source": {
                "rootDocument": f"doc-{i - 1}" if i else None,
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
    random.seed(a=777)
    for i in random.sample(list(range(n)), k=n):
        ne_id = f"named-entity-{i}"
        mention_norm = f"mention-{i // 3}"
        category = "Location" if i % 3 == 0 else "Person"
        extractor = "spacy" if i % 3 == 1 else "core-nlp"
        parent = f"doc-{i - i % 3}"
        yield {
            "_id": ne_id,
            "_source": {
                "join": {"name": "NamedEntity", "parent": parent},
                "type": "NamedEntity",
                "offsets": list(range(i + 1)),
                "extractor": extractor,
                "extractorLanguage": "en",
                "category": category,
                "mentionNorm": mention_norm,
                "mention": ne_id,
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


async def wipe_db(session: neo4j.AsyncSession):
    # Indices and constraints
    query = "CALL apoc.schema.assert({}, {})"
    await session.run(query)
    # Documents
    query = """MATCH (n)
DETACH DELETE n
    """
    await session.run(query)


async def populate_es_with_doc_and_named_entities(
    es_test_client_module: ESClient, n: int
):
    es_client = es_test_client_module
    index_name = TEST_INDEX
    # Index some Documents
    async for _ in index_docs(es_client, index_name=index_name, n=n):
        pass
    # Index entities
    async for _ in index_named_entities(es_client, index_name=index_name, n=n):
        pass


def assert_content(path: Path, expected_content: Union[bytes, str], sort_lines=False):
    if isinstance(expected_content, bytes):
        if sort_lines:
            with path.open("rb") as f:
                content = b"".join(sorted(f))
        else:
            expected_content = path.read_bytes()
    elif isinstance(expected_content, str):
        if sort_lines:
            with path.open() as f:
                content = "".join(sorted(f))
        else:
            content = path.read_text()
    else:
        raise TypeError(f"Expected Union[bytes, str], found: {expected_content}")

    assert content == expected_content


def xml_elements_equal(actual, expected) -> bool:
    if actual.tag != expected.tag:
        return False
    if actual.text != expected.text:
        return False
    if actual.tail != expected.tail:
        return False
    if actual.attrib != expected.attrib:
        return False
    if len(actual) != len(expected):
        return False
    return all(xml_elements_equal(c1, c2) for c1, c2 in zip(actual, expected))


@contextlib.contextmanager
def fail_if_exception(msg: Optional[str] = None):
    try:
        yield
    except Exception as e:  # pylint: disable=W0703
        trace = "".join(traceback.format_exception(None, e, e.__traceback__))
        if msg is None:
            msg = "Test failed due to the following error"
        pytest.fail(f"{msg}\n{trace}")
