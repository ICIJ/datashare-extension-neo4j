# pylint: disable=redefined-outer-name
import abc
import asyncio
import contextlib
import functools
import os
import random
import tempfile
import traceback
from copy import copy
from datetime import datetime
from pathlib import Path
from time import monotonic, sleep
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
)

import neo4j
import pytest
import pytest_asyncio
from elasticsearch.helpers import async_streaming_bulk
from fastapi import APIRouter, FastAPI
from neo4j import AsyncGraphDatabase
from starlette.testclient import TestClient

import neo4j_app
from neo4j_app.app import ServiceConfig
from neo4j_app.app.dependencies import (
    config_enter,
    loggers_enter,
    mp_context_enter,
    write_async_app_config_enter,
    write_async_app_config_exit,
)
from neo4j_app.app.utils import create_app
from neo4j_app.core.elasticsearch import ESClient, ESClientABC
from neo4j_app.core.elasticsearch.client import PointInTime
from neo4j_app.core.neo4j import MIGRATIONS
from neo4j_app.core.neo4j.migrations.migrate import init_project
from neo4j_app.core.neo4j.projects import NEO4J_COMMUNITY_DB
from neo4j_app.core.utils.pydantic import BaseICIJModel
from neo4j_app.icij_worker import AsyncApp, WorkerType
from neo4j_app.icij_worker.typing_ import Dependency
from neo4j_app.tasks.dependencies import (
    config_from_path_enter,
    create_project_registry_db_enter,
    es_client_enter,
    es_client_exit,
    lifespan_config,
    migrate_app_db_enter,
    neo4j_driver_enter,
    neo4j_driver_exit,
)
from neo4j_app.tests.icij_worker.conftest import (
    DBMixin,
    MockEventPublisher,
    MockManager,
    MockServiceConfig,
    MockWorkerConfig,
)
from neo4j_app.typing_ import PercentProgress

# TODO: at a high level it's a waste to have to repeat code for each fixture level,
#  let's try to find a way to define the scope dynamically:
#  https://docs.pytest.org/en/6.2.x/fixture.html#dynamic-scope


APP = AsyncApp(name="test-app")

DATA_DIR = Path(__file__).parents[3].joinpath(".data")
TEST_PROJECT = "test_project"
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


def true_after(
    state_statement: Callable, *, after_s: float, sleep_s: float = 0.01
) -> bool:
    start = monotonic()
    while "waiting for the statement to be True":
        try:
            assert state_statement()
            return True
        except AssertionError:
            if monotonic() - start < after_s:
                sleep(sleep_s)
                continue
            return False


async def async_true_after(
    state_statement: Callable[[], Awaitable[bool]],
    *,
    after_s: float,
    sleep_s: float = 0.01,
) -> bool:
    start = monotonic()
    while "waiting for the statement to be True":
        try:
            assert await state_statement()
            return True
        except AssertionError:
            if monotonic() - start < after_s:
                await asyncio.sleep(sleep_s)
                continue
            return False


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


@pytest.fixture(scope="session")
def mock_db_session() -> Path:
    with tempfile.NamedTemporaryFile(prefix="mock-db", suffix=".json") as f:
        db_path = Path(f.name)
        DBMixin.fresh_db(db_path)
        yield db_path


@pytest.fixture
def mock_db(mock_db_session: Path) -> Path:
    # Wipe the DB
    DBMixin.fresh_db(mock_db_session)
    return mock_db_session


# Define a session level even_loop fixture to overcome limitation explained here:
# https://github.com/tortoise/tortoise-orm/issues/638#issuecomment-830124562
@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


_MOCKED_HTTP_DEPS = None


@pytest.fixture(scope="session")
def test_config(mock_db_session: Path) -> ServiceConfig:
    global _MOCKED_HTTP_DEPS
    _MOCKED_HTTP_DEPS = _mock_http_deps(mock_db_session)
    config = MockServiceConfig(
        elasticsearch_address=f"http://127.0.0.1:{ELASTICSEARCH_TEST_PORT}",
        es_default_page_size=5,
        neo4j_app_async_app=f"{__name__}.APP",
        neo4j_app_dependencies=f"{__name__}._MOCKED_HTTP_DEPS",
        neo4j_app_host="127.0.0.1",
        neo4j_app_worker_type=WorkerType.mock,
        neo4j_password=NEO4J_TEST_PASSWORD,
        neo4j_port=NEO4J_TEST_PORT,
        neo4j_user=NEO4J_TEST_USER,
    )
    return config


def mock_task_manager_enter(db_path: Path, **_):
    import neo4j_app.app.dependencies

    config = lifespan_config()
    task_manager = MockManager(db_path, config.neo4j_app_task_queue_size)
    setattr(neo4j_app.app.dependencies, "_TASK_MANAGER", task_manager)


def mock_event_publisher_enter(db_path: Path, **_):
    import neo4j_app.app.dependencies

    event_publisher = MockEventPublisher(db_path)
    setattr(neo4j_app.app.dependencies, "_EVENT_PUBLISHER", event_publisher)


def _mock_http_deps(db_path: Path) -> List[Dependency]:
    deps = [
        ("configuration reading", config_enter, None),
        ("loggers setup", loggers_enter, None),
        (
            "write async config for workers",
            write_async_app_config_enter,
            write_async_app_config_exit,
        ),
        ("neo4j driver creation", neo4j_driver_enter, neo4j_driver_exit),
        ("neo4j project registry creation", create_project_registry_db_enter, None),
        ("neo4j DB migration", migrate_app_db_enter, None),
        ("ES client creation", es_client_enter, es_client_exit),
        (None, mp_context_enter, None),
        (
            "task manager creation",
            functools.partial(mock_task_manager_enter, db_path=db_path),
            None,
        ),
        (
            "event publisher creation",
            functools.partial(mock_event_publisher_enter, db_path=db_path),
            None,
        ),
    ]
    return deps


@pytest.fixture(scope="session")
def test_app_session(test_config: MockServiceConfig, mock_db_session: Path) -> FastAPI:
    worker_extras = {"teardown_dependencies": False}
    worker_config = MockWorkerConfig(db_path=mock_db_session)
    return create_app(
        test_config, worker_config=worker_config, worker_extras=worker_extras
    )


@pytest.fixture(scope="session")
def test_client_session(test_app_session: FastAPI) -> TestClient:
    # pylint: disable=unused-argument
    # Add a router which generates error in order to test error handling
    test_app_session.include_router(test_error_router())
    with TestClient(test_app_session) as client:
        yield client


@pytest.fixture(scope="module")
def test_client_module(
    test_client_session: TestClient,
    # Wipe ES by requiring the "function" level es client
    es_test_client_module: ESClient,
    # Same for neo4j
    neo4j_test_session_module: neo4j.AsyncSession,
) -> TestClient:
    # pylint: disable=unused-argument
    return test_client_session


@pytest.fixture()
def test_client(
    test_client_session: TestClient,
    # Wipe the mock db
    mock_db: Path,
    # Wipe ES by requiring the "function" level es client
    es_test_client: ESClient,
    # Same for neo4j
    neo4j_test_session: neo4j.AsyncSession,
) -> TestClient:
    # pylint: disable=unused-argument
    return test_client_session


@pytest.fixture()
def test_client_with_async(
    # Wipe ES by requiring the "function" level es client
    es_test_client: ESClient,
    # Same for neo4j
    neo4j_test_session: neo4j.AsyncSession,
    test_config: MockServiceConfig,
    mock_db: Path,
) -> Generator[TestClient, None, None]:
    # pylint: disable=unused-argument
    # Let's recreate the app to wipe the worker pool and queues
    worker_extras = {"teardown_dependencies": False}
    worker_config = MockWorkerConfig(db_path=mock_db)
    app = create_app(
        test_config, worker_config=worker_config, worker_extras=worker_extras
    )
    app.include_router(test_error_router())
    with TestClient(app) as client:
        yield client


def test_error_router() -> APIRouter:
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
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
    yield es
    await es.close()


@pytest_asyncio.fixture(scope="module")
async def es_test_client_module() -> AsyncGenerator[ESClient, None]:
    es = _make_test_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
    yield es
    await es.close()


@pytest_asyncio.fixture()
async def es_test_client() -> AsyncGenerator[ESClient, None]:
    es = _make_test_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
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


@pytest_asyncio.fixture()
async def neo4j_app_driver(neo4j_test_driver: neo4j.AsyncDriver) -> neo4j.AsyncDriver:
    await init_project(
        neo4j_test_driver,
        name=TEST_PROJECT,
        registry=MIGRATIONS,
        timeout_s=0.001,
        throttle_s=0.001,
    )
    return neo4j_test_driver


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


def make_docs(n: int, add_dates: bool = False) -> Generator[Dict, None, None]:
    random.seed(a=777)
    for i in random.sample(list(range(n)), k=n):
        root = f"doc-{i - 1}" if i else None
        doc = {
            "_index": TEST_PROJECT,
            "_id": f"doc-{i}",
            "_source": {
                "rootDocument": root,
                "dirname": f"dirname-{i}",
                "contentType": f"content-type-{i}",
                "contentLength": i**2,
                "extractionDate": "2023-02-06T13:48:22.3866",
                "extractionLevel": int(bool(root)),
                "path": f"dirname-{i}",
                "type": "Document",
                "join": {"name": "Document"},
            },
        }
        if add_dates:
            doc["_source"]["metadata"] = {
                "tika_metadata_dcterms_created_iso8601": "2022-04-08T11:41:34Z",
                "tika_metadata_modified_iso8601": "2022-04-08T11:41:34Z",
            }
        yield doc


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


def index_docs_ops(
    *, index_name: str, n: int, add_dates: bool = False
) -> Generator[Dict, None, None]:
    for doc in make_docs(n, add_dates):
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
    client: ESClient, *, n: int, index_name: str = TEST_PROJECT, add_dates: bool = False
) -> AsyncGenerator[Dict, None]:
    ops = index_docs_ops(index_name=index_name, n=n, add_dates=add_dates)
    # Let's wait to make this operation visible to the search
    refresh = "wait_for"
    async for res in async_streaming_bulk(client, actions=ops, refresh=refresh):
        yield res


async def index_noise(
    client: ESClient,
    *,
    n: int,
    index_name: str = TEST_PROJECT,
) -> AsyncGenerator[Dict, None]:
    ops = index_noise_ops(index_name=index_name, n=n)
    # Let's wait to make this operation visible to the search
    refresh = "wait_for"
    async for res in async_streaming_bulk(client, actions=ops, refresh=refresh):
        yield res


async def index_named_entities(
    client: ESClient, *, n: int, index_name: str = TEST_PROJECT
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
    index_name = TEST_PROJECT
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
        neo4j_app.core.neo4j.projects, "project_db_session", _mocked_project_db_session
    )
    monkeypatch.setattr(
        neo4j_app.core.neo4j.migrations.migrate,
        "project_db_session",
        _mocked_project_db_session,
    )
    monkeypatch.setattr(
        neo4j_app.core.neo4j.projects,
        "project_registry_db",
        _mocked_project_registry_db,
    )
    monkeypatch.setattr(
        neo4j_app.core.neo4j.projects, "is_enterprise", mocked_is_enterprise
    )


@APP.task
async def hello_world(greeted: str, progress: Optional[PercentProgress] = None) -> str:
    if progress is not None:
        await progress(0.1)
    greeting = f"Hello {greeted} !"
    if progress is not None:
        await progress(0.99)
    return greeting


@APP.task
def hello_world_sync(greeted: str) -> str:
    greeting = f"Hello {greeted} !"
    return greeting


@APP.task
async def sleep_for(
    duration: float, s: float = 0.01, progress: Optional[PercentProgress] = None
):
    start = datetime.now()
    elapsed = 0
    while elapsed < duration:
        elapsed = (datetime.now() - start).total_seconds()
        await asyncio.sleep(s)
        if progress is not None:
            await progress(elapsed / duration * 100)


@pytest.fixture(scope="session")
def test_async_app(test_config: MockServiceConfig) -> AsyncApp:
    return AsyncApp.load(test_config.neo4j_app_async_app)


@pytest.fixture()
def reset_env():
    old_env = copy(dict(os.environ))
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_env)
