import neo4j
import pytest
import pytest_asyncio

from neo4j_app.constants import NE_CATEGORY, NE_MENTION_NORM
from neo4j_app.core.elasticsearch.to_neo4j import (
    es_to_neo4j_named_entity_row,
)
from neo4j_app.core.elasticsearch.utils import SOURCE
from neo4j_app.core.neo4j.named_entities import (
    import_named_entity_rows,
    ne_creation_stats_tx,
)
from neo4j_app.tests.conftest import fail_if_exception, make_named_entities


@pytest_asyncio.fixture(scope="function")
async def _create_document(neo4j_test_session: neo4j.AsyncSession):
    await neo4j_test_session.run('CREATE (doc:Document {id: "docId"} )')
    return neo4j_test_session


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


# TODO: update this list to be exhaustive
_MATCH_SENT_EMAIL = "MATCH (ne:NamedEntity:EMAIL)-[rel:SENT]->(doc:Document) RETURN *"
_MATCH_RECEIVED_EMAIL = (
    "MATCH (ne:NamedEntity:EMAIL)-[rel:RECEIVED]->(doc:Document) RETURN *"
)


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "header_field,match_email_query",
    [
        ("tika_metadata_message_from", _MATCH_SENT_EMAIL),
        ("tika_metadata_message_bcc", _MATCH_RECEIVED_EMAIL),
        ("tika_metadata_message_cc", _MATCH_RECEIVED_EMAIL),
        ("tika_metadata_message_to", _MATCH_RECEIVED_EMAIL),
    ],
)
async def test_import_named_entity_rows_should_import_email_relationship(
    _create_document: neo4j.AsyncSession, header_field: str, match_email_query: str
):
    # pylint: disable=invalid-name
    # Given
    neo4j_session = _create_document
    records = [
        {
            "id": "senderId",
            "documentId": "docId",
            "category": "EMAIL",
            "mentionNorm": "dev@icij.org",
            "offsets": [0],
            "extractor": "fromNoWhere",
            "metadata": {"emailHeaderField": header_field},
        }
    ]
    transaction_batch_size = 2
    # When
    await import_named_entity_rows(
        neo4j_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )
    res = await neo4j_session.run(match_email_query)
    records = [rec async for rec in res]
    # Then
    assert len(records) == 1
    record = records[0]
    ne = record.get("ne")
    assert ne["mentionNorm"] == "dev@icij.org"
    doc = record.get("doc")
    assert doc["id"] == "docId"
    rel = record.get("rel")
    assert rel["fields"] == [header_field]


@pytest.mark.asyncio()
async def test_import_named_entity_rows_should_import_records_with_null_email_header(
    _create_document: neo4j.AsyncSession,
):
    # pylint: disable=invalid-name
    # Given
    neo4j_session = _create_document
    records = [
        {
            "id": "senderId",
            "documentId": "docId",
            "category": "EMAIL",
            "mentionNorm": "dev@icij.org",
            "offsets": [0],
            "extractor": "fromNoWhere",
            "metadata": {"emailHeaderField": None},
        }
    ]
    transaction_batch_size = 2
    # When
    msg = "Failed to import create email relationship when email header is None"
    with fail_if_exception(msg):
        await import_named_entity_rows(
            neo4j_session,
            records=records,
            transaction_batch_size=transaction_batch_size,
        )


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "header_field,match_email_query",
    [
        ("tika_metadata_message_from", _MATCH_SENT_EMAIL),
        ("tika_metadata_message_to", _MATCH_RECEIVED_EMAIL),
    ],
)
async def test_import_named_entity_rows_should_aggregate_email_headers(
    _create_document: neo4j.AsyncSession, header_field: str, match_email_query: str
):
    # pylint: disable=invalid-name
    # Given
    neo4j_session = _create_document
    transaction_batch_size = 2
    query = """MATCH (doc:Document { id: "docId" })
CREATE (ne:NamedEntity:EMAIL { mentionNorm: "dev@icij.org" })
CREATE (ne)-[:RECEIVED { fields: ["someField"] }]->(doc)
CREATE (ne)-[:SENT { fields: ["someField"] }]->(doc)
"""
    await neo4j_session.run(query)
    records = [
        {
            "id": "senderId",
            "documentId": "docId",
            "category": "EMAIL",
            "mentionNorm": "dev@icij.org",
            "offsets": [0],
            "extractor": "fromNoWhere",
            "metadata": {"emailHeaderField": header_field},
        }
    ]

    # When
    await import_named_entity_rows(
        neo4j_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )
    res = await neo4j_session.run(match_email_query)
    records = [rec async for rec in res]

    # Then
    assert len(records) == 1
    record = records[0]
    rel = record.get("rel")
    assert set(rel["fields"]) == {"someField", header_field}
