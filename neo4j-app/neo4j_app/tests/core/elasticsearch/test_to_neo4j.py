from neo4j_app.core.elasticsearch.to_neo4j import es_to_neo4j_named_entity_row


def test_es_to_neo4j_named_entity_row_should_contain_metadata():
    # Given
    es_document = {
        "_id": "someId",
        "_source": {
            "mentionNorm": "dev@icij.org",
            "metadata": {"emailHeader": "someHeader"},
            "join": {"parent": "docId"},
        },
    }
    # When
    neo4j_row = es_to_neo4j_named_entity_row(es_document)[0]
    # Then
    assert "metadata" in neo4j_row
