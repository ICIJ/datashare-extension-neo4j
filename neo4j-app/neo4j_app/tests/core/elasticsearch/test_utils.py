import pytest

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.elasticsearch.utils import match_all_query
from neo4j_app.tests.conftest import index_docs


@pytest.mark.asyncio
async def test_async_scan(es_test_client: ESClient):
    # Given
    index_name = es_test_client.project_index
    n_docs = 10
    scroll_size = 3
    async for _ in index_docs(es_test_client, index_name=index_name, n=n_docs):
        pass

    # When
    query = match_all_query()
    scanned_docs = [
        d
        async for d in es_test_client.async_scan(
            query=query, scroll="1m", scroll_size=scroll_size
        )
    ]

    # Then
    assert len(scanned_docs) == n_docs
