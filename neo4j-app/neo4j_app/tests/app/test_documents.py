from typing import Dict, Optional

import pytest
import pytest_asyncio
from starlette.testclient import TestClient

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.objects import DocumentImportResponse
from neo4j_app.tests.conftest import index_docs


@pytest_asyncio.fixture()
async def _index_samples_docs(es_test_client: ESClient):
    index_name = es_test_client.project_index
    n_docs = 10
    async for _ in index_docs(es_test_client, index_name=index_name, n=n_docs):
        pass


@pytest.mark.parametrize(
    "query,expected_response",
    [
        # Without query
        (None, DocumentImportResponse(n_docs_to_insert=10, n_inserted_docs=10)),
        # With query
        (
            {"term": {"_id": "doc-2"}},
            DocumentImportResponse(n_docs_to_insert=1, n_inserted_docs=1),
        ),
    ],
)
def test_post_documents_import_should_return_200(
    test_client: TestClient,
    _index_samples_docs,
    query: Optional[Dict],
    expected_response: DocumentImportResponse,
):
    # pylint: disable=invalid-name
    # Given
    url = "/documents"
    payload = {}
    if query is not None:
        payload["query"] = query

    # When
    res = test_client.post(url, json=payload)

    # Then
    assert res.status_code == 200, res.json()  # Should it be 200 or 201
    assert res.json() == expected_response.dict()
