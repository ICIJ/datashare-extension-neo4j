from typing import AsyncGenerator, Dict, Optional

import neo4j
import pytest
import pytest_asyncio

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.imports import import_documents, import_named_entities
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.tests.conftest import (
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
        (
            None,
            "type",
            IncrementalImportResponse(
                imported=20, nodes_created=20, relationships_created=19
            ),
        ),
        # Match all query, let's check that only documents are inserted and not noise
        (
            {"match_all": {}},
            "type",
            IncrementalImportResponse(
                imported=20, nodes_created=20, relationships_created=19
            ),
        ),
        # Term query, let's check that only the right doc is inserted
        (
            {"ids": {"values": ["doc-0"]}},
            "type",
            IncrementalImportResponse(
                imported=1, nodes_created=1, relationships_created=0
            ),
        ),
        # Let's check that the doc_type_field is taken into account
        (
            None,
            "fieldThatDoesNotExists",
            IncrementalImportResponse(),
        ),
    ],
)
async def test_import_documents(
    _populate_es: ESClient,
    query: Optional[Dict],
    doc_type_field: str,
    expected_response: IncrementalImportResponse,
    neo4j_test_driver: neo4j.AsyncDriver,
):
    # pylint: disable=invalid-name
    # Given
    es_client = _populate_es
    neo4j_driver = neo4j_test_driver
    # There are 20 records, let's insert by batch of 5 with transactions of 3 by batch
    neo4j_import_batch_size = 5
    max_records_in_memory = 10
    neo4j_transaction_batch_size = 3

    # When
    response = await import_documents(
        es_client=es_client,
        es_query=query,
        es_keep_alive="10s",
        es_doc_type_field=doc_type_field,
        neo4j_driver=neo4j_driver,
        neo4j_import_batch_size=neo4j_import_batch_size,
        neo4j_transaction_batch_size=neo4j_transaction_batch_size,
        max_records_in_memory=max_records_in_memory,
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
            IncrementalImportResponse(
                imported=12,
                nodes_created=int(12 / 3 * 2),
                relationships_created=int(12 / 3 * 2),
            ),
        ),
        # Match all query, let's check that only ents with doc are inserted
        (
            {"match_all": {}},
            "type",
            IncrementalImportResponse(
                imported=12,
                nodes_created=int(12 / 3 * 2),
                relationships_created=int(12 / 3 * 2),
            ),
        ),
        # Term query, let's check that only the right entity is inserted
        (
            {"ids": {"values": ["named-entity-0"]}},
            "type",
            IncrementalImportResponse(
                imported=1,
                nodes_created=1,
                relationships_created=1,
            ),
        ),
        # Let's check that the doc_type_field is taken into account
        (
            None,
            "fieldThatDoesNotExists",
            IncrementalImportResponse(
                imported=0, nodes_created=0, relationships_created=0
            ),
        ),
    ],
)
async def test_import_named_entities(
    _populate_es: ESClient,
    insert_docs_in_neo4j: neo4j.AsyncSession,
    neo4j_test_driver_session: neo4j.AsyncDriver,
    # Wipe neo4j named entities at each test_client
    wipe_named_entities,
    query: Optional[Dict],
    doc_type_field: str,
    expected_response: IncrementalImportResponse,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    es_client = _populate_es
    neo4j_driver = neo4j_test_driver_session
    # There are 20 records, let's insert by batch of 5 with transactions of 3 by batch
    neo4j_import_batch_size = 5
    max_records_in_memory = 10
    neo4j_transaction_batch_size = 3

    # When
    response = await import_named_entities(
        es_client=es_client,
        es_query=query,
        es_keep_alive="10s",
        es_doc_type_field=doc_type_field,
        neo4j_driver=neo4j_driver,
        neo4j_import_batch_size=neo4j_import_batch_size,
        neo4j_transaction_batch_size=neo4j_transaction_batch_size,
        max_records_in_memory=max_records_in_memory,
    )

    # Then
    assert response == expected_response


@pytest.mark.asyncio
async def test_should_aggregate_named_entities_attributes_on_relationship(
    _populate_es: ESClient,
    insert_docs_in_neo4j: neo4j.AsyncSession,
    neo4j_test_driver_session: neo4j.AsyncDriver,
    # Wipe neo4j named entities at each test_client
    wipe_named_entities,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    num_ent = 3
    query = {"ids": {"values": [f"named-entity-{i}" for i in range(num_ent)]}}
    es_client = _populate_es
    neo4j_driver = neo4j_test_driver_session
    neo4j_import_batch_size = 1
    max_records_in_memory = 10
    neo4j_transaction_batch_size = 1

    # When
    await import_named_entities(
        es_client=es_client,
        es_query=query,
        es_keep_alive="10s",
        es_doc_type_field="type",
        neo4j_driver=neo4j_driver,
        neo4j_import_batch_size=neo4j_import_batch_size,
        neo4j_transaction_batch_size=neo4j_transaction_batch_size,
        max_records_in_memory=max_records_in_memory,
    )
    query = "MATCH (:NamedEntity)-[rel]->(:Document) RETURN rel ORDER BY rel.ids"
    neo4j_session = insert_docs_in_neo4j
    res = await neo4j_session.run(query)
    rels = [dict(rel["rel"].items()) async for rel in res]

    # Then
    expected_rels = [
        {
            "offsets": [0],
            "mentionExtractors": ["core-nlp"],
            "mentionIds": ["named-entity-0"],
        },
        {
            "offsets": [0, 1, 2],
            "mentionExtractors": ["spacy", "core-nlp"],
            "mentionIds": ["named-entity-1", "named-entity-2"],
        },
    ]
    assert rels == expected_rels
