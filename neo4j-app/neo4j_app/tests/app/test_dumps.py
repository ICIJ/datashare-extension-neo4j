import functools
from typing import Any, AsyncGenerator, Dict, Iterable, Literal, Optional, Tuple, Union
from unittest.mock import call, patch

import pytest
from aiohttp.test_utils import TestClient
from neo4j import Query


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
    **kwargs: Any
):
    return _iter_records(lines, record_key)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "format,record_key,query,expected_run_args",
    [
        (
            "graphml",
            "data",
            None,
            call(
                """CALL apoc.export.graphml.all(null, $config)
YIELD data
RETURN data;
""",
                config={
                    "format": "gephi",
                    "batchSize": 20000,
                    "stream": True,
                    "readLabels": True,
                    "storeNodeIds": True,
                },
            ),
        ),
        (
            "graphml",
            "data",
            "MATCH doc:Document RETURN doc;",
            call(
                """CALL apoc.export.graphml.query($query_filter, null, $config)
YIELD data
RETURN data;
""",
                config={
                    "format": "gephi",
                    "batchSize": 20000,
                    "stream": True,
                    "readLabels": True,
                    "storeNodeIds": True,
                },
                query_filter="MATCH doc:Document RETURN doc;",
            ),
        ),
        (
            "cypher-shell",
            "cypherStatements",
            None,
            call(
                """CALL apoc.export.cypher.all(null, $config)
YIELD cypherStatements
RETURN cypherStatements;
""",
                config={
                    "format": "cypher-shell",
                    "cypherFormat": "create",
                    "streamStatements": True,
                    "batchSize": 20000,
                    "useOptimizations": {
                        "type": "UNWIND_BATCH",
                        "unwindBatchSize": 100,
                    },
                },
            ),
        ),
        (
            "cypher-shell",
            "cypherStatements",
            "MATCH doc:Document RETURN doc;",
            call(
                """CALL apoc.export.cypher.query($query_filter, null, $config)
YIELD cypherStatements
RETURN cypherStatements;
""",
                config={
                    "format": "cypher-shell",
                    "cypherFormat": "create",
                    "streamStatements": True,
                    "batchSize": 20000,
                    "useOptimizations": {
                        "type": "UNWIND_BATCH",
                        "unwindBatchSize": 100,
                    },
                },
                query_filter="MATCH doc:Document RETURN doc;",
            ),
        ),
    ],
)
async def test_post_graph_dump_should_return_200(
    test_client_module: TestClient,
    format: str,
    query: Optional[str],
    record_key: str,
    expected_run_args: Tuple[Tuple, ...],
):
    # pylint: disable=invalid-name,unused-argument
    # Given
    test_client = test_client_module
    url = "/graphs/dump?database=neo4j"
    payload = {"query": query, "format": format}
    exported_lines = ["exported\n", "lines"]

    # When/Then
    expected_output = b"exported\nlines"
    mocked_session_run = functools.partial(
        _mocked_run, lines=exported_lines, record_key=record_key
    )
    with patch(
        "neo4j_app.core.neo4j.dumps.neo4j.AsyncSession.run",
        spec=True,
    ) as mocked_neo4j_run:
        mocked_neo4j_run.side_effect = mocked_session_run
        res = test_client.post(url, json=payload)
        assert res.status_code == 200, res.json()
        expected_run_args = expected_run_args
        assert mocked_neo4j_run.call_args_list == [expected_run_args]
        assert res.content == expected_output


@pytest.mark.asyncio
async def test_post_graph_dump_should_return_400_for_missing_database():
    assert False


@pytest.mark.asyncio
async def test_post_graph_dump_should_return_400_for_missing_database():
    assert False
