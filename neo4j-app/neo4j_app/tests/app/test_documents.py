from typing import Dict, Optional

import neo4j
import pytest
import pytest_asyncio
from starlette.testclient import TestClient

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.tests.conftest import TEST_INDEX, index_docs


@pytest_asyncio.fixture(scope="module")
async def _populate_es(es_test_client_module: ESClient):
    es_client = es_test_client_module
    index_name = TEST_INDEX
    n_docs = 10
    async for _ in index_docs(es_client, index_name=index_name, n=n_docs):
        pass


@pytest.mark.parametrize(
    "query,expected_response",
    [
        # Without query
        (
            None,
            IncrementalImportResponse(
                imported=10, nodes_created=10, relationships_created=9
            ),
        ),
        # With query
        (
            {"term": {"_id": "doc-2"}},
            IncrementalImportResponse(
                imported=1, nodes_created=2, relationships_created=1
            ),
        ),
    ],
)
def test_post_documents_import_should_return_200(
    test_client_module: TestClient,
    # Wipe the docs after each test
    neo4j_test_session: neo4j.AsyncSession,
    _populate_es,
    query: Optional[Dict],
    expected_response: IncrementalImportResponse,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    test_client = test_client_module
    url = f"/documents?database=neo4j&index={TEST_INDEX}"
    payload = {}
    if query is not None:
        payload["query"] = query

    # When
    res = test_client.post(url, json=payload)

    # Then
    assert res.status_code == 200, res.json()  # Should it be 200 or 201
    assert res.json() == expected_response.dict()
