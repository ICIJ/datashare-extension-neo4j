import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator, Collection, Dict, List, Mapping, Optional, Type

import pytest
import pytest_asyncio
from elasticsearch import TransportError
from tenacity import RetryCallState, Retrying

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.elasticsearch.client import PointInTime, _retry_if_error_code
from neo4j_app.core.elasticsearch.utils import HITS, SCROLL_ID_, SORT
from neo4j_app.core.neo4j import get_neo4j_csv_writer
from neo4j_app.tests.conftest import (
    ELASTICSEARCH_TEST_PORT,
    TEST_INDEX,
    fail_if_exception,
    index_noise,
)


@pytest_asyncio.fixture(scope="module")
async def _index_noise(es_test_client_module: ESClient) -> ESClient:
    es_client = es_test_client_module
    n_noise = 22
    async for _ in index_noise(es_client, index_name=TEST_INDEX, n=n_noise):
        pass
    yield es_client


def _noise_to_neo4j(noise_hit: Dict) -> List[Dict[str, str]]:
    noise = {"noiseId": noise_hit["_id"]}
    hit_source = noise_hit["_source"]
    noise["someAttribute"] = hit_source["someAttribute"]
    return [noise]


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
        async with es_client.try_open_pit(index=TEST_INDEX, keep_alive="1m") as pit:
            total_hits, _ = await es_client.write_concurrently_neo4j_csvs(
                TEST_INDEX,
                query,
                pit=pit,
                nodes_f=f,
                nodes_header=header,
                keep_alive="2m",
                concurrency=concurrency,
                to_neo4j_nodes=_noise_to_neo4j,
                relationships_f=None,
                relationships_header=None,
                to_neo4j_relationships=None,
            )
        f.flush()
        with (tmp_path / "import.csv").open() as rf:
            num_lines = sum(1 for _ in rf)

    # Then
    assert total_hits == expected_num_lines - 1
    assert num_lines == expected_num_lines


class _MockFailingESClient(ESClient):
    _returned = {SCROLL_ID_: "scroll-0", HITS: {HITS: [{SORT: None}]}}
    _last_returned = {SCROLL_ID_: "scroll-0", HITS: {HITS: []}}

    def __init__(
        self,
        hits: int,
        failure: Optional[Exception],
        fail_at: List[int] = None,
        **kwargs,
    ):
        hosts = [{"host": "localhost", "port": ELASTICSEARCH_TEST_PORT}]
        super().__init__(pagination=1, hosts=hosts, **kwargs)
        self._n_calls: int = 0
        self._n_returned: int = 0
        if fail_at is None:
            fail_at = []
        self._fail_at = set(fail_at)
        self._failure = failure
        self._hits = hits

    async def search(self, **kwargs) -> Dict[str, Any]:
        # pylint: disable=arguments-differ
        return await self._call(**kwargs)

    async def scroll(self, **kwargs) -> Any:
        # pylint: disable=arguments-differ
        return await self._call(**kwargs)

    async def clear_scroll(self, **kwargs) -> Any:
        # pylint: disable=arguments-differ
        pass

    @asynccontextmanager
    async def try_open_pit(
        self, *, index: str, keep_alive: str, **kwargs
    ) -> AsyncGenerator[Optional[PointInTime], None]:
        yield dict()

    async def _call(self, **kwargs) -> Dict[str, Any]:
        # pylint: disable=unused-argument
        if isinstance(self._failure, Exception) and self._n_calls in self._fail_at:
            self._n_calls += 1
            raise self._failure
        self._n_calls += 1
        self._n_returned += 1
        returned = (
            self._last_returned if self._n_returned >= self._hits else self._returned
        )
        return returned


def _make_transport_error(error_code: int) -> TransportError:
    return TransportError(error_code, "", "")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fail_at,max_retries,failure,raised",
    [
        # No failure
        (None, 10, None, None),
        # 0 retries
        ([0], 0, _make_transport_error(429), TransportError),
        # Failure at the first call (which is different when polling/scrolling),
        # recovery
        ([0], 2, _make_transport_error(429), None),
        # Failure after the first call, recovery
        ([1], 2, _make_transport_error(429), None),
        # Recurring failure after the first call, exceeds retries
        (list(range(1, 100)), 2, _make_transport_error(429), TransportError),
        # Recurring failure after the first call, recovery
        ([1, 2], 10, _make_transport_error(429), None),
        # A non recoverable transport error
        ([1, 2], 10, _make_transport_error(400), TransportError),
    ],
)
async def test_poll_search_pages_should_retry(
    fail_at: Optional[int],
    max_retries: int,
    failure: Optional[Exception],
    raised: Type[Exception],
):
    # Given
    n_hits = 3
    client = _MockFailingESClient(
        max_retries=max_retries,
        max_retry_wait_s=int(1e-6),
        hits=n_hits,
        fail_at=fail_at,
        failure=failure,
    )

    # When/Then
    pages = client.poll_search_pages(index="", body={})

    if raised is not None:
        with pytest.raises(raised):
            _ = [h async for h in pages]
    else:
        with fail_if_exception(msg="Failed to retrieve search hits"):
            hits = [h async for h in pages]
        assert len(hits) == n_hits


@pytest.mark.parametrize(
    "error_codes,raised,expected_should_retry",
    [
        ([], TransportError(429), False),
        ([], ValueError(), False),
        ([429], TransportError(429), True),
        ([400, 429], TransportError(400), True),
        ([400, 429], TransportError(401), False),
    ],
)
def test_retry_if_error_code(
    error_codes: Collection[int],
    raised: Optional[Exception],
    expected_should_retry: bool,
):
    # Given
    retry = _retry_if_error_code(error_codes)
    retry_state = RetryCallState(Retrying(), None, None, None)
    if raised:
        try:
            raise raised
        except:  # pylint: disable=bare-except
            retry_state.set_exception(sys.exc_info())

    # When
    should_retry = retry(retry_state)

    # Then
    assert should_retry == expected_should_retry
