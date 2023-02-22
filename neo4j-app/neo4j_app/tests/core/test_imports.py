from typing import AsyncGenerator, Dict, Optional

import neo4j
import pytest
import pytest_asyncio

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.imports import import_documents, import_named_entities
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.tests.conftest import (
    NEO4J_TEST_IMPORT_DIR,
    index_docs,
    index_named_entities,
    index_noise,
)


@pytest_asyncio.fixture(scope="module")
async def _populate_es(
    es_test_client_module: ESClient,
) -> AsyncGenerator[ESClient, None]:
    es_client = es_test_client_module
    index_name = es_client.project_index
    n = 20
    # Index some Documents
    async for _ in index_docs(es_client, index_name=index_name, n=n):
        pass
    # Index entities
    async for _ in index_named_entities(es_client, index_name=index_name, n=n):
        pass
    # Index other noise
    async for _ in index_noise(es_client, index_name=index_name, n=n):
        pass
    yield es_client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query,doc_type_field,expected_response",
    [
        # No query, let's check that only documents are inserted and not noise
        (None, "type", IncrementalImportResponse(n_to_insert=20, n_inserted=20)),
        # Match all query, let's check that only documents are inserted and not noise
        (
            {"match_all": {}},
            "type",
            IncrementalImportResponse(n_to_insert=20, n_inserted=20),
        ),
        # Term query, let's check that only the right doc is inserted
        (
            {"ids": {"values": ["doc-0"]}},
            "type",
            IncrementalImportResponse(n_to_insert=1, n_inserted=1),
        ),
        # Let's check that the doc_type_field is taken into account
        (
            None,
            "fieldThatDoesNotExists",
            IncrementalImportResponse(n_to_insert=0, n_inserted=0),
        ),
    ],
)
async def test_import_documents(
    _populate_es: ESClient,
    query: Optional[Dict],
    doc_type_field: str,
    expected_response: IncrementalImportResponse,
    neo4j_test_session: neo4j.AsyncSession,
):
    # pylint: disable=invalid-name
    # Given
    es_client = _populate_es
    neo4j_session = neo4j_test_session

    # When
    response = await import_documents(
        neo4j_session=neo4j_session,
        es_client=es_client,
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR,
        query=query,
        doc_type_field=doc_type_field,
        keep_alive="10s",
    )

    # Then
    assert response == expected_response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query,doc_type_field,expected_response",
    [
        # No query, let's check that only ents with doc are inserted
        (
            None,
            "type",
            IncrementalImportResponse(n_to_insert=10, n_inserted=10),
        ),
        # Match all query, let's check that only ents with doc are inserted
        (
            {"match_all": {}},
            "type",
            IncrementalImportResponse(n_to_insert=10, n_inserted=10),
        ),
        # Term query, let's check that only the right entity is inserted
        (
            {"ids": {"values": ["named-entity-0"]}},
            "type",
            IncrementalImportResponse(n_to_insert=1, n_inserted=1),
        ),
        # Let's check that the doc_type_field is taken into account
        (
            None,
            "fieldThatDoesNotExists",
            IncrementalImportResponse(n_to_insert=0, n_inserted=0),
        ),
    ],
)
async def test_import_named_entities(
    _populate_es: ESClient,
    insert_docs_in_neo4j: neo4j.AsyncSession,
    # Wipe neo4j named entities at each test_client
    wipe_named_entities,
    query: Optional[Dict],
    doc_type_field: str,
    expected_response: IncrementalImportResponse,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    es_client = _populate_es
    neo4j_session = insert_docs_in_neo4j

    # When
    response = await import_named_entities(
        neo4j_session=neo4j_session,
        es_client=es_client,
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR,
        query=query,
        doc_type_field=doc_type_field,
        keep_alive="10s",
    )

    # Then
    assert response == expected_response
