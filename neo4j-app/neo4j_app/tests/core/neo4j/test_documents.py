import io
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import neo4j
import pytest
from neo4j.time import DateTime

from neo4j_app.core.elasticsearch.to_neo4j import es_to_neo4j_doc_row
from neo4j_app.core.neo4j import write_neo4j_csv
from neo4j_app.core.neo4j.documents import (
    import_document_rows,
)
from neo4j_app.tests.conftest import (
    make_docs,
)


async def test_write_neo4j_csv():
    # Given

    docs = make_docs(n=3)
    f = io.StringIO()
    headers = [
        "id",
        "rootDocument",
        "dirname",
        "contentType",
        "contentLength",
        "extractionDate",
        "urlSuffix",
        "path",
    ]

    # When
    docs = (row for doc in docs for row in es_to_neo4j_doc_row(doc))
    write_neo4j_csv(f, rows=docs, header=headers, write_header=True)
    csv = f.getvalue()

    # Then
    expected_csv = """id,rootDocument,dirname,contentType,contentLength,\
extractionDate,urlSuffix,path
doc-0,,dirname-0,content-type-0,0,2023-02-06T13:48:22.3866,\
ds/test_project/doc-0/doc-0,dirname-0
doc-1,doc-0,dirname-1,content-type-1,1,2023-02-06T13:48:22.3866,\
ds/test_project/doc-1/doc-0,dirname-1
doc-2,doc-1,dirname-2,content-type-2,4,2023-02-06T13:48:22.3866,\
ds/test_project/doc-2/doc-1,dirname-2
"""
    assert csv == expected_csv


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
        records = [row for doc in docs[:n_existing] for row in es_to_neo4j_doc_row(doc)]
        counters = await import_document_rows(
            neo4j_session=neo4j_test_session,
            records=records,
            transaction_batch_size=transaction_batch_size,
        )
        n_created_first = counters.nodes_created
    records = [row for doc in docs for row in es_to_neo4j_doc_row(doc)]
    counters = await import_document_rows(
        neo4j_session=neo4j_test_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )
    n_created_second = counters.nodes_created

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
    records = [row for doc in docs for row in es_to_neo4j_doc_row(doc)]
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
    assert len(doc) == 7
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


_datetime_0 = datetime.utcnow().replace(microsecond=0)
_datetime_1 = _datetime_0 + timedelta(0, 1)
_datetime_2 = _datetime_1 + timedelta(0, 1)
_datetime_3 = _datetime_2 + timedelta(0, 1)
_datetime_4 = _datetime_3 + timedelta(0, 1)


def _tika_iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat() + "Z"


@pytest.mark.parametrize(
    "metadata,expected_created_at",
    [
        (None, None),
        (dict(), None),
        (
            {
                "tika_metadata_date_iso8601": _tika_iso(_datetime_0),
            },
            _datetime_0.replace(tzinfo=timezone.utc),
        ),
        (
            {
                "tika_metadata_creation_date_iso8601": _tika_iso(_datetime_0),
                "tika_metadata_date_iso8601": _tika_iso(_datetime_1),
            },
            _datetime_0.replace(tzinfo=timezone.utc),
        ),
        (
            {
                "tika_metadata_dcterms_created_iso8601": _tika_iso(_datetime_0),
                "tika_metadata_creation_date_iso8601": _tika_iso(_datetime_1),
                "tika_metadata_date_iso8601": _tika_iso(_datetime_2),
            },
            _datetime_0.replace(tzinfo=timezone.utc),
        ),
    ],
)
async def test_import_documents_should_add_created_at(
    neo4j_test_session: neo4j.AsyncSession,
    metadata: Optional[Dict],
    expected_created_at: Optional[datetime],
):
    # Given
    transaction_batch_size = 10
    docs = list(make_docs(n=1))
    if metadata is not None:
        for d in docs:
            d["_source"].update({"metadata": metadata})

    # When
    records = [row for doc in docs for row in es_to_neo4j_doc_row(doc)]
    await import_document_rows(
        neo4j_session=neo4j_test_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )

    # Then
    query = "MATCH (doc:Document) RETURN doc"
    res = await neo4j_test_session.run(query)
    doc = await res.single(strict=True)
    doc = doc["doc"]
    created_at = doc.get("createdAt")
    if created_at is not None:
        created_at = created_at.to_native()
    assert created_at == expected_created_at


@pytest.mark.parametrize(
    "metadata,expected_modified_at",
    [
        (None, None),
        (dict(), None),
        (
            {
                "tika_metadata_date_iso8601": _tika_iso(_datetime_0),
            },
            _datetime_0.replace(tzinfo=timezone.utc),
        ),
        (
            {
                "tika_metadata_modified_iso8601": _tika_iso(_datetime_0),
                "tika_metadata_date_iso8601": _tika_iso(_datetime_1),
            },
            _datetime_0.replace(tzinfo=timezone.utc),
        ),
        (
            {
                "tika_metadata_dcterms_modified_iso8601": _tika_iso(_datetime_0),
                "tika_metadata_modified_iso8601": _tika_iso(_datetime_1),
                "tika_metadata_date_iso8601": _tika_iso(_datetime_2),
            },
            _datetime_0.replace(tzinfo=timezone.utc),
        ),
    ],
)
async def test_import_documents_should_add_modified_at(
    neo4j_test_session: neo4j.AsyncSession,
    metadata: Optional[Dict],
    expected_modified_at: Optional[datetime],
):
    # Given
    transaction_batch_size = 10
    docs = list(make_docs(n=1))
    if metadata is not None:
        for d in docs:
            d["_source"].update({"metadata": metadata})

    # When
    records = [row for doc in docs for row in es_to_neo4j_doc_row(doc)]
    await import_document_rows(
        neo4j_session=neo4j_test_session,
        records=records,
        transaction_batch_size=transaction_batch_size,
    )

    # Then
    query = "MATCH (doc:Document) RETURN doc"
    res = await neo4j_test_session.run(query)
    doc = await res.single(strict=True)
    doc = doc["doc"]
    modified_at = doc.get("modifiedAt")
    if modified_at is not None:
        modified_at = modified_at.to_native()
    assert modified_at == expected_modified_at
