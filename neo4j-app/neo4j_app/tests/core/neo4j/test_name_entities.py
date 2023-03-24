import neo4j
import pytest

from neo4j_app.core.elasticsearch.to_neo4j import (
    es_to_neo4j_named_entity,
)
from neo4j_app.core.neo4j.named_entities import import_named_entity_rows
from neo4j_app.tests.conftest import (
    make_named_entities,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("n_existing", list(range(3)))
async def test_import_named_entities(
    neo4j_test_session: neo4j.AsyncSession, n_existing: int
):
    # Given
    transaction_batch_size = 3
    num_ents = 3
    ents = list(make_named_entities(n=num_ents))

    # When
    n_created_first = 0
    if n_existing:
        records = [es_to_neo4j_named_entity(ent) for ent in ents[:n_existing]]
        summary = await import_named_entity_rows(
            neo4j_test_session,
            records=records,
            transaction_batch_size=transaction_batch_size,
        )
        n_created_first = summary.counters.nodes_created
    records = [es_to_neo4j_named_entity(ent) for ent in ents]
    summary = await import_named_entity_rows(
        neo4j_test_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )
    n_created_second = summary.counters.nodes_created

    # Then
    assert n_created_first == n_existing
    assert n_created_second == num_ents - n_existing
    query = """
MATCH (ent:NamedEntity)
RETURN ent as ent
ORDER BY ent.id"""
    res = await neo4j_test_session.run(query)
    ents = [dict(rec["ent"]) async for rec in res]
    expected_ents = [
        {"id": "named-entity-0", "offsets": [1], "documentId": "doc-0"},
        {"id": "named-entity-1", "offsets": [1], "documentId": "doc-1"},
        {"id": "named-entity-2", "offsets": [1], "documentId": "doc-2"},
    ]
    assert ents == expected_ents


@pytest.mark.asyncio
async def test_import_named_entities_should_update_named_entity(
    neo4j_test_session: neo4j.AsyncSession,
):
    # Given
    num_ents = 1
    transaction_batch_size = 3
    ents = list(make_named_entities(n=num_ents))
    query = """
CREATE (n:NamedEntity {id: 'named-entity-0', offsets: [1, 2], documentId: 'doc-0'})
"""
    await neo4j_test_session.run(query)

    # When
    records = [es_to_neo4j_named_entity(ent) for ent in ents]
    await import_named_entity_rows(
        neo4j_test_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )

    # Then
    query = """
MATCH (ent:NamedEntity)
RETURN ent as ent"""
    res = await neo4j_test_session.run(query)
    ent = await res.single()
    ent = dict(ent["ent"])
    expected_ent = {"id": "named-entity-0", "offsets": [1], "documentId": "doc-0"}
    assert ent == expected_ent
