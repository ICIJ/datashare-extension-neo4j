from typing import Dict, Optional
from urllib.parse import quote_plus

import pytest

from neo4j_app.core.elasticsearch.to_neo4j import (
    _parse_doc_title,
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


@pytest.mark.parametrize(
    "source,expected_title",
    [
        # Title should be the document ID up to 10 chars when there is nothing else
        ({}, "doc-id"),
        # Then it should use the last part of the document path
        ({"path": "/some/path"}, "path"),
        ({"path": "/some/path/"}, "doc-id"),
        # But it should discard empty ones and default to doc ID instead
        ({"path": "/"}, "doc-id"),
        ({"path": ""}, "doc-id"),
        # Then it should use resource name when level > 0
        (
            {
                "path": "/some/path",
                "metadata": {"tika_metadata_resourcename": "resource-name"},
                "extractionLevel": 1,
            },
            "resource-name",
        ),
        (
            {
                "path": "/some/path",
                "metadata": {"tika_metadata_resourcename": "resource-name"},
                "extractionLevel": 0,
            },
            "path",
        ),
        (
            {
                "path": "/some/path",
                "metadata": {"tika_metadata_resourcename": "resource-name"},
            },
            "path",
        ),
        # and trim it
        (
            {
                "path": "/some/path",
                "metadata": {"tika_metadata_resourcename": " resource-name "},
                "extractionLevel": 1,
            },
            "resource-name",
        ),
        # It should also handle URL encoded resource names
        (
            {
                "path": "/some/path",
                "metadata": {
                    "tika_metadata_resourcename": f"=?{quote_plus('named=name')}?="
                },
                "extractionLevel": 1,
            },
            "named=name",
        ),
        # But fallback to the doc path when it's empty
        (
            {"path": "/some/path", "metadata": {"tika_metadata_resourcename": " "}},
            "path",
        ),
        # Then it should use the doc title
        (
            {
                "path": "/some/path",
                "metadata": {"tika_metadata_resourcename": "resource-name"},
                "title": "some-title",
            },
            "some-title",
        ),
        # But fallback to the doc resource name when it's empty
        (
            {
                "path": "/some/path",
                "metadata": {"tika_metadata_resourcename": "resource-name"},
                "title": " ",
                "extractionLevel": 1,
            },
            "resource-name",
        ),
        # Then it should use email title for emails
        (
            {
                "path": "/some/path",
                "title": "some-title",
                "contentType": "message/some-email-type",
                "metadata": {"tika_metadata_dc_title": "email-title"},
            },
            "email-title",
        ),
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "application/vnd.ms-outlook",  # outlook content type
                "metadata": {"tika_metadata_dc_title": "email-title"},
            },
            "email-title",
        ),
        # and should trim it
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "message/some-email-type",
                "metadata": {"tika_metadata_dc_title": " email-title "},
            },
            "email-title",
        ),
        # and should fall back to title when it's empty
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "message/some-email-type",
                "metadata": {"tika_metadata_dc_title": " "},
            },
            "some-title",
        ),
        # Then it should use email dc subject
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "message/some-email-type",
                "metadata": {
                    "tika_metadata_dc_title": "email-title",
                    "tika_metadata_dc_subject": "email-dc-subject",
                },
            },
            "email-dc-subject",
        ),
        # and fallback to email title when empty
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "message/some-email-type",
                "metadata": {
                    "tika_metadata_dc_title": "email-title",
                    "tika_metadata_dc_subject": " ",
                },
            },
            "email-title",
        ),
        # Then it should use email dc subject
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "message/some-email-type",
                "metadata": {
                    "tika_metadata_dc_title": "email-title",
                    "tika_metadata_dc_subject": "email-dc-subject",
                    "tika_metadata_subject": "email-subject",
                },
            },
            "email-subject",
        ),
        # and fallback to email title when empty
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "message/some-email-type",
                "metadata": {
                    "tika_metadata_dc_title": "email-title",
                    "tika_metadata_dc_subject": "email-dc-subject",
                    "tika_metadata_subject": " ",
                },
            },
            "email-title",
        ),
        # then it should use dc title for tweets
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "application/json; twint",
                "metadata": {
                    "tika_metadata_dc_title": " tweet-title ",
                },
            },
            "tweet-title",
        ),
        # and fallback to title when it's empty
        (
            {
                "path": "/some/path",
                "tika_metadata_resourcename": "resource-name",
                "title": "some-title",
                "contentType": "application/json; twint",
                "metadata": {
                    "tika_metadata_dc_title": " ",
                },
            },
            "some-title",
        ),
    ],
)
def test_parse_doc_title(source: Optional[Dict], expected_title: str):
    # Given
    es_document = {
        "_id": "doc-id",
        "_index": TEST_PROJECT,
        "_source": source,
    }
    # When
    title = _parse_doc_title(es_document)  # pylint: disable=protected-access
    # Then
    assert title == expected_title
