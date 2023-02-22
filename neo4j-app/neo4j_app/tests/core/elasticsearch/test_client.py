from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import pytest
import pytest_asyncio

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.neo4j import get_neo4j_csv_writer
from neo4j_app.tests.conftest import index_noise


@pytest_asyncio.fixture(scope="module")
async def _index_noise(es_test_client_module: ESClient) -> ESClient:
    es_client = es_test_client_module
    n_noise = 22
    async for _ in index_noise(
        es_client, index_name=es_client.project_index, n=n_noise
    ):
        pass
    yield es_client


def _noise_to_neo4j(noise_hit: Dict) -> Dict[str, str]:
    noise = {"noiseId": noise_hit["_id"]}
    hit_source = noise_hit["_source"]
    noise["someAttribute"] = hit_source["someAttribute"]
    return noise


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query,concurrency,expected_num_lines",
    [
        # No concurrency (should default to 5 which is the client default)
        (None, None, 22 + 1),
        # Concurrency 1 (should actually use 2)
        (None, 1, 22 + 1),
        # Higher concurrency
        (None, 3, 22 + 1),
        # With query
        (
            {"query": {"ids": {"values": [f"noise-{i}" for i in range(0, 22, 2)]}}},
            3,
            11 + 1,
        ),
    ],
)
async def test_write_concurrently_neo4j_csv(
    _index_noise: ESClient,
    tmp_path: Path,
    query: Optional[Mapping[str, Any]],
    concurrency: Optional[int],
    expected_num_lines: int,
):
    # pylint: disable=invalid-name
    # Given
    es_client = _index_noise
    header = ["noiseId", "someAttribute"]

    # When
    with (tmp_path / "import.csv").open("w") as f:
        writer = get_neo4j_csv_writer(f, header)
        writer.writeheader()
        total_hits = await es_client.write_concurrently_neo4j_csv(
            query,
            f,
            header=header,
            keep_alive="2m",
            concurrency=concurrency,
            es_to_neo4j=_noise_to_neo4j,
        )
        f.flush()
        with (tmp_path / "import.csv").open() as rf:
            num_lines = sum(1 for _ in rf)

    # Then
    assert total_hits == expected_num_lines - 1
    assert num_lines == expected_num_lines
