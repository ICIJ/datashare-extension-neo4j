import neo4j
import pytest

from neo4j_app.core.elasticsearch.to_neo4j import (
    es_to_neo4j_named_entity,
)
from neo4j_app.core.neo4j import make_neo4j_import_file, write_neo4j_csv
from neo4j_app.core.neo4j.named_entities import import_named_entities_from_csv_tx
from neo4j_app.tests.conftest import (
    NEO4J_TEST_IMPORT_DIR,
    make_named_entities,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("n_existing", list(range(3)))
async def test_import_named_entities(
    neo4j_test_session: neo4j.AsyncSession, n_existing: int
):
    # Given
    num_ents = 3
    ents = list(make_named_entities(n=num_ents))

    headers = [
        "id",
        "documentId",
        "offsets",
    ]
    # When
    n_created_first = 0
    if n_existing:
        with make_neo4j_import_file(
            neo4j_import_dir=NEO4J_TEST_IMPORT_DIR, neo4j_import_prefix=None
        ) as (
            f,
            neo4j_path,
        ):
            rows = (es_to_neo4j_named_entity(ent) for ent in ents[:n_existing])
            write_neo4j_csv(f, rows=rows, header=headers, write_header=True)
            f.flush()
            summary = await neo4j_test_session.execute_write(
                import_named_entities_from_csv_tx, neo4j_import_path=neo4j_path
            )
            n_created_first = summary.counters.nodes_created
    with make_neo4j_import_file(
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR, neo4j_import_prefix=None
    ) as (
        f,
        neo4j_path,
    ):
        rows = (es_to_neo4j_named_entity(ent) for ent in ents)
        write_neo4j_csv(f, rows=rows, header=headers, write_header=True)
        f.flush()
        summary = await neo4j_test_session.execute_write(
            import_named_entities_from_csv_tx, neo4j_import_path=neo4j_path
        )
        n_created_second = summary.counters.nodes_created

    # Then
    assert n_created_first == n_existing
    assert n_created_second == num_ents - n_existing
    query = """
MATCH (ent:NamedEntity)
RETURN ent as ent"""
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
    ents = list(make_named_entities(n=num_ents))
    query = """
CREATE (n:NamedEntity {id: 'named-entity-0', offsets: [1, 2], documentId: 'doc-0'})
"""
    await neo4j_test_session.run(query)

    # When
    headers = [
        "id",
        "documentId",
        "offsets",
    ]
    with make_neo4j_import_file(
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR, neo4j_import_prefix=None
    ) as (
        f,
        neo4j_path,
    ):
        rows = (es_to_neo4j_named_entity(ent) for ent in ents)
        write_neo4j_csv(f, rows=rows, header=headers, write_header=True)
        f.flush()
        await neo4j_test_session.execute_write(
            import_named_entities_from_csv_tx, neo4j_import_path=neo4j_path
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
