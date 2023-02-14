from typing import AsyncGenerator, Dict, Generator

import pytest
import pytest_asyncio
from elasticsearch.helpers import async_streaming_bulk
from starlette.testclient import TestClient

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.run.utils import create_app


@pytest.fixture(scope="session")
def test_client() -> TestClient:
    app = create_app()
    with TestClient(app) as client:
        yield client


@pytest_asyncio.fixture()
async def es_test_client() -> AsyncGenerator[ESClient, None]:
    # Since we're using elasticmock we don't really mind the host
    test_index = "test_index"
    es = ESClient(project_index=test_index, hosts=[{"host": "localhost", "port": 9200}])
    await es.indices.delete(index="_all")
    await es.indices.create(index=test_index)
    yield es
    await es.close()


def make_docs(n: int) -> Generator[Dict, None, None]:
    for i in range(n):
        yield {"content": f"this is document number {i}"}


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
