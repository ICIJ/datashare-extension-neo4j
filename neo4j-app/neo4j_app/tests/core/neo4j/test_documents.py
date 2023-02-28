import io

import neo4j
import pytest
from neo4j.time import DateTime

from neo4j_app.core.elasticsearch.to_neo4j import es_to_neo4j_doc
from neo4j_app.core.neo4j import make_neo4j_import_file, write_neo4j_csv
from neo4j_app.core.neo4j.documents import (
    import_documents_from_csv_tx,
)
from neo4j_app.tests.conftest import (
    NEO4J_IMPORT_PREFIX,
    NEO4J_TEST_IMPORT_DIR,
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
    docs = (es_to_neo4j_doc(doc) for doc in docs)
    write_neo4j_csv(f, rows=docs, header=headers, write_header=True)
    csv = f.getvalue()

    # Then
    expected_csv = """id,rootId,dirname,contentType,contentLength,\
extractionDate,path
doc-0,,dirname-0,content-type-0,0,2023-02-06T13:48:22.3866,dirname-0
doc-1,,dirname-1,content-type-1,1,2023-02-06T13:48:22.3866,dirname-1
doc-2,,dirname-2,content-type-2,4,2023-02-06T13:48:22.3866,dirname-2
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
    n_created_first = 0
    if n_existing:
        with make_neo4j_import_file(
            neo4j_import_dir=NEO4J_TEST_IMPORT_DIR,
            neo4j_import_prefix=str(NEO4J_IMPORT_PREFIX),
        ) as (
            f,
            neo4j_path,
        ):
            rows = (es_to_neo4j_doc(doc) for doc in docs[:n_existing])
            write_neo4j_csv(f, rows=rows, header=headers, write_header=True)
            f.flush()
            summary = await neo4j_test_session.execute_write(
                import_documents_from_csv_tx, neo4j_import_path=neo4j_path
            )
            n_created_first = summary.counters.nodes_created
    with make_neo4j_import_file(
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR,
        neo4j_import_prefix=str(NEO4J_IMPORT_PREFIX),
    ) as (
        f,
        neo4j_path,
    ):
        rows = (es_to_neo4j_doc(doc) for doc in docs)
        write_neo4j_csv(f, rows=rows, header=headers, write_header=True)
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
    # TODO: test the documents directly
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
CREATE (n:Document {id: 'doc-0', contentType: 'someContentType'})
"""
    await neo4j_test_session.run(query)

    # When
    headers = [
        "id",
        "rootId",
        "dirname",
        "contentType",
        "contentLength",
        "extractionDate",
        "path",
    ]
    with make_neo4j_import_file(
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR,
        neo4j_import_prefix=str(NEO4J_IMPORT_PREFIX),
    ) as (
        f,
        neo4j_path,
    ):
        rows = (es_to_neo4j_doc(doc) for doc in docs)
        write_neo4j_csv(f, rows=rows, header=headers, write_header=True)
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
    assert len(doc) == len(headers) - 1  # for the roodId which is None
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
