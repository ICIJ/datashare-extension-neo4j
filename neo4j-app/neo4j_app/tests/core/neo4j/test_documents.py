import io

import neo4j
import pytest
from neo4j.time import DateTime

from neo4j_app.core.elasticsearch.to_neo4j import es_to_neo4j_row
from neo4j_app.core.neo4j import write_neo4j_csv
from neo4j_app.core.neo4j.documents import (
    import_document_rows,
)
from neo4j_app.tests.conftest import (
    make_docs,
)


@pytest.mark.asyncio
async def test_write_neo4j_csv():
    # Given

    docs = make_docs(n=3)
    f = io.StringIO()
    headers = [
        "id",
        "rootId",
        "dirname",
        "contentType",
        "contentLength",
        "extractionDate",
        "path",
    ]

    # When
    docs = (row for doc in docs for row in es_to_neo4j_row(doc))
    write_neo4j_csv(f, rows=docs, header=headers, write_header=True)
    csv = f.getvalue()

    # Then
    expected_csv = """id,rootId,dirname,contentType,contentLength,\
extractionDate,path
doc-0,,dirname-0,content-type-0,0,2023-02-06T13:48:22.3866,dirname-0
doc-1,doc-0,dirname-1,content-type-1,1,2023-02-06T13:48:22.3866,dirname-1
doc-2,doc-1,dirname-2,content-type-2,4,2023-02-06T13:48:22.3866,dirname-2
"""
    assert csv == expected_csv


@pytest.mark.asyncio
@pytest.mark.parametrize("n_existing", list(range(3)))
async def test_import_documents(
    neo4j_test_session: neo4j.AsyncSession, n_existing: int
):
    # Given
    num_docs = 3
    docs = list(make_docs(n=num_docs))
    # When
    n_created_first = 0
    transaction_batch_size = 3
    if n_existing:
        records = [row for doc in docs[:n_existing] for row in es_to_neo4j_row(doc)]
        summary = await import_document_rows(
            neo4j_session=neo4j_test_session,
            records=records,
            transaction_batch_size=transaction_batch_size,
        )
        n_created_first = summary.counters.nodes_created
    records = [row for doc in docs for row in es_to_neo4j_row(doc)]
    summary = await import_document_rows(
        neo4j_session=neo4j_test_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )
    n_created_second = summary.counters.nodes_created

    # Then
    assert n_created_first == n_existing
    assert n_created_second == num_docs - n_existing
    query = """
MATCH (doc:Document)
RETURN count(*) as numDocs"""
    res = await neo4j_test_session.run(query)
    # TODO: test the documents directly
    total_docs = await res.single()
    assert total_docs["numDocs"] == 3


@pytest.mark.asyncio
async def test_import_documents_should_update_document(
    neo4j_test_session: neo4j.AsyncSession,
):
    # Given
    num_docs = 1
    transaction_batch_size = 3
    docs = list(make_docs(n=num_docs))
    query = """
CREATE (n:Document {id: 'doc-0', contentType: 'someContentType'})
"""
    await neo4j_test_session.run(query)

    # When
    records = [row for doc in docs for row in es_to_neo4j_row(doc)]
    await import_document_rows(
        neo4j_session=neo4j_test_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
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
    assert len(doc) == 6
    assert doc["id"] == expected_doc["_id"]
    ignored = {"type", "join"}
    # TODO: test the document directly
    for k, v in expected_doc["_source"].items():
        if k in ignored:
            continue
        if v is None:
            assert k not in doc
            continue
        doc_property = doc[k]
        if k == "extractionDate":
            assert isinstance(doc_property, DateTime)
        else:
            assert doc_property == v
