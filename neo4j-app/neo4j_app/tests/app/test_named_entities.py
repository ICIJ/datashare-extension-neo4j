from typing import Dict, Optional

import neo4j
import pytest
import pytest_asyncio
from starlette.testclient import TestClient

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.tests.conftest import (
    TEST_PROJECT,
    populate_es_with_doc_and_named_entities,
)


@pytest_asyncio.fixture(scope="module")
async def _populate_es(es_test_client_module: ESClient):
    await populate_es_with_doc_and_named_entities(es_test_client_module, n=10)


@pytest.mark.parametrize(
    "query,expected_response",
    [
        # Without query
        (
            None,
            IncrementalImportResponse(
                imported=10, nodes_created=7, relationships_created=7
            ),
        ),
        # With query
        (
            {"term": {"_id": "named-entity-2"}},
            IncrementalImportResponse(
                imported=1, nodes_created=1, relationships_created=1
            ),
        ),
    ],
)
def test_post_named_entities_import_should_return_200(
    test_client_module: TestClient,
    insert_docs_in_neo4j: neo4j.AsyncSession,
    _populate_es,
    query: Optional[Dict],
    expected_response: IncrementalImportResponse,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    test_client = test_client_module
    url = f"/named-entities?project={TEST_PROJECT}"
    payload = {}
    if query is not None:
        payload["query"] = query

    # When
    res = test_client.post(url, json=payload)

    # Then
    assert res.status_code == 200, res.json()  # Should it be 200 or 201
    assert res.json() == expected_response.dict(by_alias=True)
