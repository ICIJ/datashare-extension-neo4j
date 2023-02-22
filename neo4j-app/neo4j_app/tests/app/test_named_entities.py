from typing import Dict, Optional

import neo4j
import pytest
import pytest_asyncio
from starlette.testclient import TestClient

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.tests.conftest import index_docs, index_named_entities


@pytest_asyncio.fixture(scope="module")
async def _populate_es(es_test_client_module: ESClient):
    es_client = es_test_client_module
    index_name = es_client.project_index
    n = 10
    # Index some Documents
    async for _ in index_docs(es_client, index_name=index_name, n=n):
        pass
    # Index entities
    async for _ in index_named_entities(es_client, index_name=index_name, n=n):
        pass


@pytest.mark.parametrize(
    "query,expected_response",
    [
        # Without query
        (None, IncrementalImportResponse(n_to_insert=10, n_inserted=10)),
        # With query
        (
            {"term": {"_id": "named-entity-2"}},
            IncrementalImportResponse(n_to_insert=1, n_inserted=1),
        ),
    ],
)
def test_post_documents_import_should_return_200(
    test_client_module: TestClient,
    insert_docs_in_neo4j: neo4j.AsyncSession,
    _populate_es,
    query: Optional[Dict],
    expected_response: IncrementalImportResponse,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    test_client = test_client_module
    url = "/named-entities"
    payload = {}
    if query is not None:
        payload["query"] = query

    # When
    res = test_client.post(url, json=payload)

    # Then
    assert res.status_code == 200, res.json()  # Should it be 200 or 201
    assert res.json() == expected_response.dict()
