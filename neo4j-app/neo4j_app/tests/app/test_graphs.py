import functools
from typing import Any, AsyncGenerator, Dict, Iterable, Optional, Tuple, Union
from unittest.mock import call, patch

import pytest
from aiohttp.test_utils import TestClient
from icij_common.test_utils import TEST_PROJECT, fail_if_exception
from neo4j import Query

from neo4j_app.core.objects import GraphCounts


async def _iter_records(
    lines: Iterable[str],
    record_key: str,
) -> AsyncGenerator[Dict, None]:
    for l in lines:
        record = {record_key: l.encode()}
        yield record


async def _mocked_run(
    query: Union[str, Query],
    lines: Iterable[str],
    record_key: str,
    parameters: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
):
    # pylint: disable=unused-argument
    return _iter_records(lines, record_key)


@pytest.mark.parametrize(
    "dump_format,record_key,query,expected_run_args",
    [
        (
            "graphml",
            "data",
            None,
            call(
                """
CALL apoc.export.graphml.query($query_filter, null, $config) YIELD data
RETURN data;
""",
                config={
                    "format": "gephi",
                    "stream": True,
                    "streamStatements": True,
                    "readLabels": False,
                    "storeNodeIds": False,
                },
                query_filter="""MATCH (doc:Document)
WITH doc
ORDER BY doc.path ASC
OPTIONAL MATCH (doc)-[rel:APPEARS_IN|SENT|RECEIVED]-(ne:NamedEntity)
RETURN apoc.coll.toSet(collect(doc) + collect(ne) + collect(rel)) AS values""",
            ),
        ),
        (
            "cypher-shell",
            "cypherStatements",
            "MATCH doc:Document RETURN doc;",
            call(
                """
CALL apoc.export.cypher.query($query_filter, null, $config) YIELD cypherStatements
RETURN cypherStatements;
""",
                config={
                    "stream": True,
                    "streamStatements": True,
                    "writeNodeProperties": True,
                    "format": "cypher-shell",
                    "cypherFormat": "create",
                    "useOptimizations": {
                        "type": "UNWIND_BATCH",
                        "unwindBatchSize": 1000,
                    },
                },
                query_filter="MATCH doc:Document RETURN doc;",
            ),
        ),
        (
            "cypher-shell",
            "cypherStatements",
            None,
            call(
                """
CALL apoc.export.cypher.query($query_filter, null, $config) YIELD cypherStatements
RETURN cypherStatements;
""",
                config={
                    "stream": True,
                    "streamStatements": True,
                    "writeNodeProperties": True,
                    "format": "cypher-shell",
                    "cypherFormat": "create",
                    "useOptimizations": {
                        "type": "UNWIND_BATCH",
                        "unwindBatchSize": 1000,
                    },
                },
                query_filter="""MATCH (doc:Document)
WITH doc
ORDER BY doc.path ASC
OPTIONAL MATCH (doc)-[rel:APPEARS_IN|SENT|RECEIVED]-(ne:NamedEntity)
RETURN apoc.coll.toSet(collect(doc) + collect(ne) + collect(rel)) AS values""",
            ),
        ),
        (
            "cypher-shell",
            "cypherStatements",
            "MATCH doc:Document RETURN doc;",
            call(
                """
CALL apoc.export.cypher.query($query_filter, null, $config) YIELD cypherStatements
RETURN cypherStatements;
""",
                config={
                    "stream": True,
                    "streamStatements": True,
                    "writeNodeProperties": True,
                    "format": "cypher-shell",
                    "cypherFormat": "create",
                    "useOptimizations": {
                        "type": "UNWIND_BATCH",
                        "unwindBatchSize": 1000,
                    },
                },
                query_filter="MATCH doc:Document RETURN doc;",
            ),
        ),
    ],
)
async def test_post_graph_dump_should_return_200(
    test_client_module: TestClient,
    dump_format: str,
    query: Optional[str],
    record_key: str,
    expected_run_args: Tuple[Tuple, ...],
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    test_client = test_client_module
    url = "/graphs/dump?project={TEST_PROJECT}"
    payload = {"query": query, "format": dump_format}
    exported_lines = ["exported\n", "lines"]

    # When/Then
    expected_output = b"exported\nlines"
    mocked_session_run = functools.partial(
        _mocked_run, lines=exported_lines, record_key=record_key
    )
    with patch(
        "neo4j_app.core.neo4j.graphs.neo4j.AsyncSession.run",
        spec=True,
    ) as mocked_neo4j_run:
        mocked_neo4j_run.side_effect = mocked_session_run
        res = test_client.post(url, json=payload)
        assert res.status_code == 200, res.json()
        assert mocked_neo4j_run.call_args_list == [expected_run_args]
        assert res.content == expected_output


async def test_post_graph_dump_should_return_400_for_missing_database(
    test_client_module: TestClient,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    test_client = test_client_module
    url = "/graphs/dump"
    payload = {"query": None, "format": "graphml"}

    # When/Then
    res = test_client.post(url, json=payload)
    assert res.status_code == 400, res.json()
    error = res.json()
    assert error["title"] == "Request Validation Error"
    assert "field required" in error["detail"]


async def test_post_graph_dump_should_return_400_for_invalid_dump_format(
    test_client_module: TestClient,
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    test_client = test_client_module
    url = "/graphs/dump?project={TEST_PROJECT}"
    payload = {"query": None, "format": "idontexist"}

    # When/Then
    res = test_client.post(url, json=payload)
    assert res.status_code == 400, res.json()
    error = res.json()
    assert error["title"] == "Request Validation Error"
    assert "value is not a valid enumeration member" in error["detail"]


def test_get_graph_counts(test_client_session: TestClient):
    # Given
    test_client = test_client_session
    url = f"/graphs/counts?project={TEST_PROJECT}"

    # When/Then
    res = test_client.get(url)
    assert res.status_code == 200, res.json()
    msg = f"Failed to convert response into a {GraphCounts.__name__}"
    with fail_if_exception(msg):
        _ = GraphCounts(**res.json())
