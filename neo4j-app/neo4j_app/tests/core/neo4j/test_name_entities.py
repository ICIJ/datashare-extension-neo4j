import neo4j
import pytest

from neo4j_app.constants import NE_CATEGORY, NE_MENTION_NORM
from neo4j_app.core.elasticsearch.to_neo4j import (
    es_to_neo4j_named_entity_row,
)
from neo4j_app.core.elasticsearch.utils import SOURCE
from neo4j_app.core.neo4j.named_entities import (
    import_named_entity_rows,
    ne_creation_stats_tx,
)
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
    expected_n_nodes = 2
    ents = list(make_named_entities(n=num_ents))

    # When
    n_existing_nodes = 0
    if n_existing:
        created_first = ents[:n_existing]
        keys = set(
            (
                (e[SOURCE][NE_MENTION_NORM], e[SOURCE][NE_CATEGORY])
                for e in created_first
            )
        )
        n_existing_nodes = len(keys)
        records = [
            row for ent in created_first for row in es_to_neo4j_named_entity_row(ent)
        ]
        await import_named_entity_rows(
            neo4j_test_session,
            records=records,
            transaction_batch_size=transaction_batch_size,
        )
    records = [row for ent in ents for row in es_to_neo4j_named_entity_row(ent)]
    n_created_first, _ = await neo4j_test_session.execute_read(ne_creation_stats_tx)
    await import_named_entity_rows(
        neo4j_test_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )
    n_nodes, _ = await neo4j_test_session.execute_read(ne_creation_stats_tx)

    # Then
    assert n_created_first == n_existing_nodes
    n_created_second = n_nodes - n_existing_nodes
    assert n_created_second == expected_n_nodes - n_created_first
    query = """
MATCH (ent:NamedEntity)
RETURN ent as ent, apoc.coll.sort(labels(ent)) as entLabels
ORDER BY entLabels"""
    res = await neo4j_test_session.run(query)
    ents = [(rec["ent"]["mentionNorm"], rec["entLabels"]) async for rec in res]
    expected_ents = [
        ("mention-0", ["Location", "NamedEntity"]),
        ("mention-0", ["NamedEntity", "Person"]),
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
    records = [row for ent in ents for row in es_to_neo4j_named_entity_row(ent)]
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
    expected_ent = {"id": "named-entity-0", "offsets": [1, 2], "documentId": "doc-0"}
    assert ent == expected_ent
