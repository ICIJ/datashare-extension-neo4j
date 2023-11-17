from typing import Optional

import pytest

from neo4j_app.core.elasticsearch.to_neo4j import (
    es_to_neo4j_doc_row,
    es_to_neo4j_named_entity_row,
)
from neo4j_app.tests.conftest import TEST_PROJECT


@pytest.mark.parametrize(
    "root_id,expected_url_suffix",
    [
        (None, "ds/test_project/doc-id/doc-id"),
        ("root-id", "ds/test_project/doc-id/root-id"),
    ],
)
def test_es_to_neo4j_doc_row_should_have_ds_url_suffix(
    root_id: Optional[str], expected_url_suffix: str
):
    # Given
    es_document = {
        "_id": "doc-id",
        "_index": TEST_PROJECT,
        "_source": {},
    }
    if root_id is not None:
        es_document["_source"]["rootDocument"] = root_id
    # When
    neo4j_row = es_to_neo4j_doc_row(es_document)[0]
    # Then
    assert "urlSuffix" in neo4j_row
    assert neo4j_row["urlSuffix"] == expected_url_suffix


def test_es_to_neo4j_named_entity_row_should_contain_metadata():
    # Given
    es_document = {
        "_id": "someId",
        "_source": {
            "mentionNorm": "dev@icij.org",
            "metadata": {"emailHeaderField": "someHeader"},
            "join": {"parent": "docId"},
        },
    }
    # When
    neo4j_row = es_to_neo4j_named_entity_row(es_document)[0]
    # Then
    assert "metadata" in neo4j_row
