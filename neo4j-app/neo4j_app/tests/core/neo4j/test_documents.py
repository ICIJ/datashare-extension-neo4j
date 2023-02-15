import io
from typing import Iterable

import neo4j
import pytest
from neo4j.time import DateTime

from neo4j_app.core.elasticsearch.documents import to_document_csv
from neo4j_app.core.neo4j.documents import (
    import_documents_from_csv_tx,
    make_neo4j_import_file,
    write_neo4j_csv,
)
from neo4j_app.tests.conftest import NEO4J_TEST_IMPORT_DIR, make_docs


async def _make_async_gen(docs: Iterable):
    for doc in docs:
        yield doc


@pytest.mark.asyncio
async def test_write_neo4j_csv():
    # Given

    docs = _make_async_gen(make_docs(n=3))
    f = io.StringIO()
    headers = [
        "documentId",
        "rootId",
        "dirname",
        "contentType",
        "contentLength",
        "extractionDate",
        "path",
    ]

    # When
    await write_neo4j_csv(f, rows=to_document_csv(docs), header=headers)
    csv = f.getvalue()

    # Then
    expected_csv = """documentId,rootId,dirname,contentType,contentLength,\
extractionDate,path
doc-0,,dirname-0,content-type-0,0,2023-02-06T13:48:22.3866,dirname-0
doc-1,,dirname-1,content-type-1,1,2023-02-06T13:48:22.3866,dirname-1
doc-2,,dirname-2,content-type-2,4,2023-02-06T13:48:22.3866,dirname-2
"""
    assert csv == expected_csv


@pytest.mark.asyncio
@pytest.mark.parametrize("n_existing", list(range(3)))
async def test_import_documents_from_empty_db(
    neo4j_test_session: neo4j.AsyncSession, n_existing: int
):
    # Given
    num_docs = 3
    docs = list(make_docs(n=num_docs))

    headers = [
        "documentId",
        "rootId",
        "dirname",
        "contentType",
        "contentLength",
        "extractionDate",
        "path",
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
            rows = to_document_csv(_make_async_gen(docs[:n_existing]))
            await write_neo4j_csv(f, rows=rows, header=headers)
            f.flush()
            summary = await neo4j_test_session.execute_write(
                import_documents_from_csv_tx, neo4j_import_path=neo4j_path
            )
            n_created_first = summary.counters.nodes_created
    with make_neo4j_import_file(
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR, neo4j_import_prefix=None
    ) as (
        f,
        neo4j_path,
    ):
        await write_neo4j_csv(
            f, rows=to_document_csv(_make_async_gen(docs)), header=headers
        )
        f.flush()
        summary = await neo4j_test_session.execute_write(
            import_documents_from_csv_tx, neo4j_import_path=neo4j_path
        )
        n_created_second = summary.counters.nodes_created

    # Then
    assert n_created_first == n_existing
    assert n_created_second == num_docs - n_existing
    query = """
MATCH (doc:Document)
RETURN count(*) as numDocs"""
    res = await neo4j_test_session.run(query)
    total_docs = await res.single()
    assert total_docs["numDocs"] == 3


@pytest.mark.asyncio
async def test_import_documents_should_update_document(
    neo4j_test_session: neo4j.AsyncSession,
):
    # Given
    num_docs = 1
    docs = list(make_docs(n=num_docs))
    query = """
CREATE (n:Document {documentId: 'doc-0', contentType: 'someContentType'})
"""
    await neo4j_test_session.run(query)

    # When
    headers = [
        "documentId",
        "rootId",
        "dirname",
        "contentType",
        "contentLength",
        "extractionDate",
        "path",
    ]
    with make_neo4j_import_file(
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR, neo4j_import_prefix=None
    ) as (
        f,
        neo4j_path,
    ):
        await write_neo4j_csv(
            f, rows=to_document_csv(_make_async_gen(docs)), header=headers
        )
        f.flush()
        await neo4j_test_session.execute_write(
            import_documents_from_csv_tx, neo4j_import_path=neo4j_path
        )

    # Then
    query = """
MATCH (doc:Document)
RETURN doc, count(*) as numDocs"""
    res = await neo4j_test_session.run(query)
    doc = await res.single()
    count = doc["numDocs"]
    assert count == 1
    doc = dict(doc["doc"])
    expected_doc = docs[0]
    num_expected_properties = len(expected_doc["_source"]) - 1  # for the type
    assert len(doc) == num_expected_properties
    assert doc["documentId"] == expected_doc["_id"]
    for k, v in expected_doc["_source"].items():
        if k == "type":
            continue
        if v is None:
            assert k not in doc
            continue
        doc_property = doc[k]
        if k == "extractionDate":
            assert isinstance(doc_property, DateTime)
        else:
            assert doc_property == v
