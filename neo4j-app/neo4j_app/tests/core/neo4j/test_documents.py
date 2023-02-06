import io
from pathlib import Path
from tempfile import NamedTemporaryFile

import neo4j
import pytest

from neo4j.time import DateTime

from neo4j_app.core.elasticsearch.documents import to_document_csv
from neo4j_app.core.neo4j.documents import import_documents_from_csv_tx, write_neo4j_csv
from neo4j_app.tests.conftest import DATA_DIR, make_docs


def test_write_neo4j_csv():
    # Given
    docs = make_docs(n=3)
    f = io.StringIO("w", newline="")
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
    write_neo4j_csv(f, rows=to_document_csv(docs), header=headers)
    csv = f.getvalue()

    # Then
    # pylint: disable=line-too-long
    expected_csv = """documentId,rootId,dirname,contentType,contentLength,extractionDate,path
document-0,,dirname-0,content-type-0,0,2023-02-06T13:48:22.3866,dirname-0
document-1,,dirname-1,content-type-1,1,2023-02-06T13:48:22.3866,dirname-1
document-2,,dirname-2,content-type-2,4,2023-02-06T13:48:22.3866,dirname-2
"""
    # pylint: enable=line-too-long
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
    local_import_path = DATA_DIR.joinpath("neo4j", "import")
    # When
    n_created_first = 0
    if n_existing:
        with NamedTemporaryFile(
            "w", dir=str(local_import_path), suffix=".csv"
        ) as tmp_csv:
            rows = to_document_csv(docs[:n_existing])
            write_neo4j_csv(tmp_csv, rows=rows, header=headers)
            tmp_csv.flush()
            instance_import_path = Path(tmp_csv.name).name
            summary = await neo4j_test_session.execute_write(
                import_documents_from_csv_tx, csv_path=instance_import_path
            )
            n_created_first = summary.counters.nodes_created
    with NamedTemporaryFile("w", dir=str(local_import_path), suffix=".csv") as tmp_csv:
        write_neo4j_csv(tmp_csv, rows=to_document_csv(docs), header=headers)
        tmp_csv.flush()
        instance_import_path = Path(tmp_csv.name).name
        summary = await neo4j_test_session.execute_write(
            import_documents_from_csv_tx, csv_path=instance_import_path
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
CREATE (n:Document {documentId: 'document-0', contentType: 'someContentType'})
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
    local_import_path = DATA_DIR.joinpath("neo4j", "import")
    with NamedTemporaryFile("w", dir=str(local_import_path), suffix=".csv") as tmp_csv:
        write_neo4j_csv(tmp_csv, rows=to_document_csv(docs), header=headers)
        tmp_csv.flush()
        instance_import_path = Path(tmp_csv.name).name

        await neo4j_test_session.execute_write(
            import_documents_from_csv_tx, csv_path=instance_import_path
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
    num_expected_properties = sum(1 for v in expected_doc.values() if v is not None)
    assert len(doc) == num_expected_properties
    for k, v in expected_doc.items():
        if v is None:
            assert k not in doc
            continue
        doc_property = doc[k]
        if k == "extractionDate":
            assert isinstance(doc_property, DateTime)
        else:
            assert doc_property == v
