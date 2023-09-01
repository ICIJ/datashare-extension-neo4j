import itertools
import json
import os
import shutil
import subprocess
import tarfile
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional
from unittest.mock import patch

import neo4j
import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase

from neo4j_app import ROOT_DIR
from neo4j_app.constants import DOC_NODE, DOC_ROOT_REL_LABEL, NE_APPEARS_IN_DOC, NE_NODE
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.elasticsearch.to_neo4j import make_ne_hit_id
from neo4j_app.core.elasticsearch.utils import match_all
from neo4j_app.core.imports import (
    _make_document_query,
    _make_named_entity_with_parent_queries,
    import_documents,
    import_named_entities,
    to_neo4j_csvs,
)
from neo4j_app.core.objects import (
    IncrementalImportResponse,
    Neo4jCSVs,
    NodeCSVs,
    RelationshipCSVs,
)
from neo4j_app.tests.conftest import (
    NEO4J_TEST_AUTH,
    NEO4J_TEST_PORT,
    TEST_INDEX,
    assert_content,
    index_docs,
    index_named_entities,
    index_noise,
)


@pytest_asyncio.fixture(scope="module")
async def _populate_es(
    es_test_client_module: ESClient,
) -> AsyncGenerator[ESClient, None]:
    es_client = es_test_client_module
    index_name = TEST_INDEX
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
        # No query, let's check that only documents are inserted and not noise
        (
            {"ids": {"values": ["i-dont-exist"]}},
            "type",
            IncrementalImportResponse(),
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
        es_index=TEST_INDEX,
        es_client=es_client,
        es_query=query,
        es_keep_alive="10s",
        es_doc_type_field=doc_type_field,
        neo4j_driver=neo4j_driver,
        neo4j_db="neo4j",
        neo4j_import_batch_size=neo4j_import_batch_size,
        neo4j_transaction_batch_size=neo4j_transaction_batch_size,
        max_records_in_memory=max_records_in_memory,
    )

    # Then
    assert response == expected_response


@pytest.mark.asyncio
async def test_import_documents_should_forward_db(
    neo4j_test_driver_session: neo4j.AsyncDriver,
    _populate_es: ESClient,  # pylint: disable=invalid-name
):
    # pylint: disable=not-async-context-manager
    # Given
    project_db = "some_db"
    neo4j_driver = neo4j_test_driver_session
    es_client = _populate_es

    # We check that the project DB is passed all the way, while returning an actual
    # session so that the test flow can continue
    uri = f"neo4j://127.0.0.1:{NEO4J_TEST_PORT}"
    #
    async with AsyncGraphDatabase.driver(uri, auth=NEO4J_TEST_AUTH) as driver:

        @asynccontextmanager
        async def mock_session(database: Optional[str] = None):
            # Simulate an unknown DB
            if database != project_db:
                raise RuntimeError(f'Unknown DB "{database}"')
            async with driver.session(database="neo4j") as session:
                yield session

        with patch.object(neo4j_driver, attribute="session", new=mock_session):
            # When/Then
            res = await import_documents(
                es_index=TEST_INDEX,
                es_client=es_client,
                es_query=dict(),
                es_keep_alive="10s",
                es_doc_type_field="type",
                neo4j_driver=neo4j_driver,
                neo4j_db=project_db,
                neo4j_import_batch_size=10,
                neo4j_transaction_batch_size=10,
                max_records_in_memory=10,
            )
            assert res.imported


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
        # Match no ne
        (
            {"ids": {"values": ["i-dont-exist"]}},
            "type",
            IncrementalImportResponse(),
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
        es_index=TEST_INDEX,
        es_query=query,
        es_keep_alive="10s",
        es_doc_type_field=doc_type_field,
        neo4j_driver=neo4j_driver,
        neo4j_db="neo4j",
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
        es_index=TEST_INDEX,
        es_keep_alive="10s",
        es_doc_type_field="type",
        neo4j_driver=neo4j_driver,
        neo4j_db="neo4j",
        neo4j_import_batch_size=neo4j_import_batch_size,
        neo4j_transaction_batch_size=neo4j_transaction_batch_size,
        max_records_in_memory=max_records_in_memory,
    )
    query = "MATCH (:NamedEntity)-[rel]->(:Document) RETURN rel ORDER BY rel.ids"
    neo4j_session = insert_docs_in_neo4j
    res = await neo4j_session.run(query)
    rels = [dict(rel["rel"].items()) async for rel in res]
    for rel in rels:
        rel["mentionIds"] = sorted(rel["mentionIds"])
        rel["mentionExtractors"] = sorted(rel["mentionExtractors"])

    # Then
    expected_rels = [
        {
            "offsets": [0],
            "mentionExtractors": ["core-nlp"],
            "mentionIds": ["named-entity-0"],
            "extractorLanguage": "en",
        },
        {
            "offsets": [0, 1, 2],
            "mentionExtractors": ["core-nlp", "spacy"],
            "mentionIds": ["named-entity-1", "named-entity-2"],
            "extractorLanguage": "en",
        },
    ]
    assert rels == expected_rels


@pytest.mark.asyncio
async def test_import_named_entities_should_forward_db(
    neo4j_test_driver_session: neo4j.AsyncDriver,
    _populate_es: ESClient,  # pylint: disable=invalid-name
):
    # pylint: disable=not-async-context-manager
    # Given
    project_db = "some_db"
    neo4j_driver = neo4j_test_driver_session
    es_client = _populate_es

    # We check that the project DB is passed all the way, while returning an actual
    # session so that the test flow can continue
    uri = f"neo4j://127.0.0.1:{NEO4J_TEST_PORT}"
    async with AsyncGraphDatabase.driver(uri, auth=NEO4J_TEST_AUTH) as driver:

        @asynccontextmanager
        async def mock_session(database: Optional[str] = None):
            # Simulate an unknown DB
            if database != project_db:
                raise RuntimeError(f'Unknown DB "{database}"')
            async with driver.session(database="neo4j") as session:
                yield session

        with patch.object(neo4j_driver, attribute="session", new=mock_session):
            # When/Then
            await import_named_entities(
                es_client=es_client,
                es_query=dict(),
                es_index=TEST_INDEX,
                es_keep_alive="10s",
                es_doc_type_field="type",
                neo4j_driver=neo4j_driver,
                neo4j_db=project_db,
                neo4j_import_batch_size=10,
                neo4j_transaction_batch_size=10,
                max_records_in_memory=10,
            )


def _expected_ne_nodes_lines() -> str:
    data = itertools.product((f"mention-{i}" for i in range(3)), ["Person", "Location"])
    lines = (
        f"{make_ne_hit_id(mention_norm=m, category=c)},{m},{NE_NODE}|{c}"
        for m, c in data
    )
    return "\n".join(sorted(lines)) + "\n"


def _make_ne_doc_rel_line(
    *,
    mention_norm: str,
    category: str,
    doc_id: int,
    ne_ids: List[int],
    extractors: List[str],
    max_offset: int,
) -> str:
    extractors = "|".join(sorted(extractors))
    ne_ids = "|".join(f"named-entity-{i}" for i in ne_ids)
    offsets = "|".join(str(o) for o in range(max_offset))
    ne_hash = make_ne_hit_id(mention_norm=mention_norm, category=category)
    return f"{extractors},en,{ne_ids},{offsets},{ne_hash},doc-{doc_id},APPEARS_IN"


def _expected_ne_doc_rel_lines() -> str:
    # Doc 0
    l_0_0 = _make_ne_doc_rel_line(
        mention_norm="mention-0",
        category="Location",
        doc_id=0,
        ne_ids=[0],
        extractors=["core-nlp"],
        max_offset=1,
    )
    l_0_1 = _make_ne_doc_rel_line(
        mention_norm="mention-0",
        category="Person",
        doc_id=0,
        ne_ids=[1, 2],
        extractors=["core-nlp", "spacy"],
        max_offset=3,
    )
    # Doc 3
    l_3_0 = _make_ne_doc_rel_line(
        mention_norm="mention-1",
        category="Location",
        doc_id=3,
        ne_ids=[3],
        extractors=["core-nlp"],
        max_offset=4,
    )
    l_3_1 = _make_ne_doc_rel_line(
        mention_norm="mention-1",
        category="Person",
        doc_id=3,
        ne_ids=[4, 5],
        extractors=["core-nlp", "spacy"],
        max_offset=6,
    )
    # Doc 6
    l_6_0 = _make_ne_doc_rel_line(
        mention_norm="mention-2",
        category="Location",
        doc_id=6,
        ne_ids=[6],
        extractors=["core-nlp"],
        max_offset=7,
    )
    l_6_1 = _make_ne_doc_rel_line(
        mention_norm="mention-2",
        category="Person",
        doc_id=6,
        ne_ids=[7, 8],
        extractors=["core-nlp", "spacy"],
        max_offset=9,
    )

    lines = [l_0_0, l_0_1, l_3_0, l_3_1, l_6_0, l_6_1]
    return "\n".join(sorted(lines)) + "\n"


@pytest.mark.asyncio
async def test_to_neo4j_csvs(_populate_es: ESClient, tmpdir):
    # pylint: disable=line-too-long,invalid-name
    # Given
    export_dir = Path(tmpdir)
    es_doc_type_field = "type"
    es_client = _populate_es
    ids = [f"doc-{i}" for i in range(0, 3 * 3, 3)]
    # Let's add doc-1 to have at least 1 doc-root
    ids.append("doc-1")
    es_query = {"ids": {"values": ids}}

    # When
    res = await to_neo4j_csvs(
        es_query=es_query,
        es_index=TEST_INDEX,
        export_dir=export_dir,
        es_client=es_client,
        es_concurrency=None,
        es_keep_alive="1m",
        es_doc_type_field=es_doc_type_field,
        neo4j_db="neo4j",
    )
    archive_dir = export_dir.joinpath("archive")
    archive_dir.mkdir()
    with tarfile.open(res.path, "r:gz") as f:
        f.extractall(archive_dir)

    # Then
    metadata_path = archive_dir / "metadata.json"
    assert metadata_path.exists()
    metadata = Neo4jCSVs.parse_file(metadata_path)
    assert metadata == res.metadata
    assert metadata.db == "neo4j"

    assert len(metadata.nodes) == 2

    doc_nodes_export = metadata.nodes[0]
    assert doc_nodes_export.n_nodes == 4
    assert doc_nodes_export.labels == [DOC_NODE]

    expected_doc_header = """\
id:ID(Document),dirname,contentType,contentLength:LONG,extractionDate:DATETIME,path,:LABEL
"""
    doc_nodes_header_path = archive_dir / doc_nodes_export.header_path
    assert_content(doc_nodes_header_path, expected_doc_header)

    expected_doc_nodes = """doc-0,dirname-0,content-type-0,0,2023-02-06T13:48:22.3866,dirname-0,Document
doc-1,dirname-1,content-type-1,1,2023-02-06T13:48:22.3866,dirname-1,Document
doc-3,dirname-3,content-type-3,9,2023-02-06T13:48:22.3866,dirname-3,Document
doc-6,dirname-6,content-type-6,36,2023-02-06T13:48:22.3866,dirname-6,Document
"""
    doc_root_rels_path = archive_dir / doc_nodes_export.node_paths[0]
    assert_content(doc_root_rels_path, expected_doc_nodes, sort_lines=True)

    ne_nodes_export = metadata.nodes[1]
    assert ne_nodes_export.n_nodes == 3 * 2
    assert ne_nodes_export.labels == [NE_NODE]

    ne_nodes_header_path = archive_dir / ne_nodes_export.header_path
    expected_ne_header = """:ID,mentionNorm,:LABEL
"""
    assert_content(ne_nodes_header_path, expected_ne_header)

    ne_nodes_path = archive_dir / ne_nodes_export.node_paths[0]
    expected_ne = _expected_ne_nodes_lines()
    assert_content(ne_nodes_path, expected_ne, sort_lines=True)

    assert len(metadata.relationships) == 2

    doc_root_rel_export = metadata.relationships[0]
    assert doc_root_rel_export.n_relationships == 4 - 1
    assert doc_root_rel_export.types == [DOC_ROOT_REL_LABEL]

    expected_doc_root_rels_header = """:START_ID(Document),:END_ID(Document)
"""
    doc_root_rels_header_path = archive_dir / doc_root_rel_export.header_path
    assert_content(doc_root_rels_header_path, expected_doc_root_rels_header)

    expected_doc_root_rels = """doc-1,doc-0
doc-3,doc-2
doc-6,doc-5
"""
    doc_root_rels_path = archive_dir / doc_root_rel_export.relationship_paths[0]
    assert_content(doc_root_rels_path, expected_doc_root_rels, sort_lines=True)

    ne_doc_rels_export = metadata.relationships[1]
    assert ne_doc_rels_export.n_relationships == 3 * 2
    assert ne_doc_rels_export.types == [NE_APPEARS_IN_DOC]

    ne_doc_rels_header_path = archive_dir / ne_doc_rels_export.header_path
    expected_ne_doc_rels_header = """mentionExtractors:STRING[],extractorLanguage,\
mentionIds:STRING[],offsets:LONG[],:START_ID,:END_ID(Document),:TYPE
"""
    assert_content(ne_doc_rels_header_path, expected_ne_doc_rels_header)

    ne_doc_rels_path = archive_dir / ne_doc_rels_export.relationship_paths[0]
    ne_doc_rels = _expected_ne_doc_rel_lines()
    assert_content(ne_doc_rels_path, ne_doc_rels, sort_lines=True)

    assert archive_dir.joinpath("bulk-import.sh").exists()


@pytest.mark.parametrize(
    "neo4j_home,db,expected_cmd",
    [
        (
            ".",
            "some-specific-db",
            './bin/neo4j-admin database import full \
--array-delimiter="|" \
--skip-bad-relationships \
--nodes=Document="docs-header.csv,docs.csv" \
--nodes="entities-header.csv,entities.csv" \
--relationships=HAS_PARENT="doc-roots-header.csv,doc-roots.csv" \
--relationships=APPEARS_IN="entity-docs-header.csv,entity-docs.csv" \
some-specific-db\n',
        ),
        (
            "some-neo4j-home",
            "neo4j",
            'some-neo4j-home/bin/neo4j-admin database import full \
--array-delimiter="|" \
--skip-bad-relationships \
--nodes=Document="docs-header.csv,docs.csv" \
--nodes="entities-header.csv,entities.csv" \
--relationships=HAS_PARENT="doc-roots-header.csv,doc-roots.csv" \
--relationships=APPEARS_IN="entity-docs-header.csv,entity-docs.csv" \
neo4j\n',
        ),
    ],
)
def test_neo4j_bulk_import_script(
    neo4j_home: str, db: Optional[str], tmpdir, expected_cmd: str
):
    # Given
    tmpdir = Path(tmpdir)
    metadata = Neo4jCSVs(
        db=db,
        nodes=[
            NodeCSVs(
                labels=["Document"],
                header_path="docs-header.csv",
                node_paths=["docs.csv"],
                n_nodes=4,
            ),
            NodeCSVs(
                labels=[],
                header_path="entities-header.csv",
                node_paths=["entities.csv"],
                n_nodes=6,
            ),
        ],
        relationships=[
            RelationshipCSVs(
                types=["HAS_PARENT"],
                header_path="doc-roots-header.csv",
                relationship_paths=["doc-roots.csv"],
                n_relationships=3,
            ),
            RelationshipCSVs(
                types=["APPEARS_IN"],
                header_path="entity-docs-header.csv",
                relationship_paths=["entity-docs.csv"],
                n_relationships=6,
            ),
        ],
    )
    tmpdir.joinpath("metadata.json").write_text(json.dumps(metadata.dict(), indent=2))
    script_path = tmpdir.joinpath("bulk-import.sh")
    shutil.copy(ROOT_DIR.joinpath("scripts", "bulk-import.sh"), script_path)
    cmd = [script_path, "--dry-run"]
    env = deepcopy(os.environ)
    env["NEO4J_HOME"] = neo4j_home

    # When
    neo4j_cmd = subprocess.check_output(cmd, env=env).decode()

    # Then
    assert neo4j_cmd == expected_cmd


def test_make_document_query():
    # Given
    es_query = match_all()

    # When
    body = _make_document_query(es_query=es_query, es_doc_type_field="type")

    # Then
    assert "_source" in body
    assert body["_source"]


def test_make_named_entity_query():
    # Given
    es_query = match_all()

    # When
    body = _make_document_query(es_query=es_query, es_doc_type_field="type")

    # Then
    assert "_source" in body
    assert body["_source"]


def test_make_named_entity_with_parent_queries():
    # Given
    es_query = None

    # When
    bodies = _make_named_entity_with_parent_queries(
        es_query=es_query,
        document_ids=["doc-0"],
        es_pit=dict(),
        es_doc_type_field="type",
        es_page_size=10,
    )

    # Then
    assert len(bodies) == 1
    body = bodies[0]
    assert "_source" in body
    assert body["_source"]
