import os
import stat
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.objects import (
    Neo4jCSVResponse,
    Neo4jCSVs,
    NodeCSVs,
    RelationshipCSVs,
)
from neo4j_app.tests.conftest import (
    TEST_PROJECT,
    populate_es_with_doc_and_named_entities,
)


@pytest_asyncio.fixture(scope="module")
async def _populate_es(es_test_client_module: ESClient):
    await populate_es_with_doc_and_named_entities(es_test_client_module, n=10)


@pytest.mark.asyncio
async def test_post_named_entities_import_should_return_200(
    test_client_module: TestClient,
    _populate_es,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    test_client = test_client_module
    query = {"ids": {"values": ["doc-0"]}}
    url = f"/admin/neo4j-csvs?project={TEST_PROJECT}"
    payload = {"query": query}

    # When
    res = test_client.post(url, json=payload)

    # Then
    assert res.status_code == 200, res.json()
    res = Neo4jCSVResponse.parse_obj(res.json())
    assert Path(res.path).exists()
    st = os.stat(res.path)
    assert bool(st.st_mode & stat.S_IROTH)
    expected_metadata = Neo4jCSVs(
        db="neo4j",
        nodes=[
            NodeCSVs(
                labels=["Document"],
                header_path="docs-header.csv",
                node_paths=["docs.csv"],
                n_nodes=1,
            ),
            NodeCSVs(
                labels=["NamedEntity"],
                header_path="entities-header.csv",
                node_paths=["entities.csv"],
                n_nodes=2,
            ),
        ],
        relationships=[
            RelationshipCSVs(
                types=["HAS_PARENT"],
                header_path="doc-roots-header.csv",
                relationship_paths=["doc-roots.csv"],
                n_relationships=0,
            ),
            RelationshipCSVs(
                types=["APPEARS_IN"],
                header_path="entity-docs-header.csv",
                relationship_paths=["entity-docs.csv"],
                n_relationships=2,
            ),
            RelationshipCSVs(
                types=[],
                header_path="email-docs-header.csv",
                relationship_paths=["email-docs.csv"],
                n_relationships=0,
            ),
        ],
    )
    assert res.metadata == expected_metadata
