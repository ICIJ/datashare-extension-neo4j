import abc
import sys
from pathlib import Path
from typing import Any, Collection, Dict, List, Mapping, Optional, Type
from unittest.mock import patch

import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch, TransportError
from icij_common.test_utils import fail_if_exception
from opensearchpy import AsyncOpenSearch
from tenacity import RetryCallState, Retrying

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.elasticsearch.client import OSClient, _retry_if_error_code
from neo4j_app.core.elasticsearch.utils import HITS, SCROLL_ID_, SORT
from neo4j_app.core.neo4j import get_neo4j_csv_writer
from neo4j_app.tests.conftest import (
    MockedESClient,
    TEST_PROJECT,
    index_noise,
)


@pytest_asyncio.fixture(scope="module")
async def _index_noise(es_test_client_module: ESClient) -> ESClient:
    es_client = es_test_client_module
    n_noise = 22
    async for _ in index_noise(es_client, index_name=TEST_PROJECT, n=n_noise):
        pass
    yield es_client


def _noise_to_neo4j(noise_hit: Dict) -> List[Dict[str, str]]:
    noise = {"noiseId": noise_hit["_id"]}
    hit_source = noise_hit["_source"]
    noise["someAttribute"] = hit_source["someAttribute"]
    return [noise]


async def _mocked_search(*, body: Optional[Dict], index: Optional[str], size: int):
    # pylint: disable=unused-argument
    return {}


async def test_es_client_should_search_with_pagination_size():
    # Given
    pagination = 666
    es_client = ESClient(pagination=pagination)
    index = "test-datashare-project"

    # When
    with patch.object(AsyncElasticsearch, "search") as mocked_search:
        mocked_search.side_effect = _mocked_search
        await es_client.search(body=None, index=index)
        # Then
        mocked_search.assert_called_once_with(body=None, index=index, size=pagination)


async def test_os_client_should_search_with_pagination_size():
    # Given
    pagination = 666
    es_client = OSClient(pagination=pagination)
    index = "test-datashare-project"

    # When
    with patch.object(AsyncOpenSearch, "search") as mocked_search:
        mocked_search.side_effect = _mocked_search
        await es_client.search(body=None, index=index)
        # Then
        mocked_search.assert_called_once_with(body=None, index=index, size=pagination)


async def test_es_client_should_raise_when_size_is_provided():
    # Given
    pagination = 666
    es_client = ESClient(pagination=pagination)
    size = 100
    body = None
    index = "test-datashare-project"

    # When/Then
    expected_msg = "ESClient run searches using the pagination_size"
    with pytest.raises(ValueError, match=expected_msg):
        await es_client.search(body=body, index=index, size=size)


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
        async with es_client.try_open_pit(index=TEST_PROJECT, keep_alive="1m") as pit:
            total_hits, _ = await es_client.write_concurrently_neo4j_csvs(
                TEST_PROJECT,
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


class _MockFailingClient(MockedESClient, metaclass=abc.ABCMeta):
    def __init__(
        self,
        n_hits: int,
        failure: Optional[Exception],
        fail_at: List[int] = None,
        **kwargs,
    ):
        super().__init__(pagination=1, **kwargs)
        self._n_calls: int = 0
        self._n_returned: int = 0
        if fail_at is None:
            fail_at = []
        self._fail_at = set(fail_at)
        self._failure = failure
        self._n_hits = n_hits

    def _make_hits(self) -> List[Dict[str, Any]]:
        # pylint: disable=unused-argument
        if isinstance(self._failure, Exception) and self._n_calls in self._fail_at:
            self._n_calls += 1
            raise self._failure
        hits = [{SORT: None}] if self._n_returned < self._n_hits else []
        self._n_calls += 1
        self._n_returned += 1
        return hits


class _MockFailingESClient(_MockFailingClient):
    default_sort = ESClient.default_sort

    @property
    async def supports_pit(self) -> bool:
        return True

    async def _mocked_search(self, **kwargs) -> Dict[str, Any]:
        # pylint: disable=unused-argument
        hits = self._make_hits()
        return {HITS: {HITS: hits}}


class _MockFailingOSClient(_MockFailingClient):
    default_sort = OSClient.default_sort

    @property
    async def supports_pit(self) -> bool:
        return False

    async def _mocked_search(self, **kwargs) -> Dict[str, Any]:
        # pylint: disable=unused-argument
        hits = self._make_hits()
        return {SCROLL_ID_: "scroll-0", HITS: {HITS: hits}}


def _make_transport_error(error_code: int) -> TransportError:
    return TransportError(error_code, "", "")


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
@pytest.mark.parametrize("client_cls", [_MockFailingESClient, _MockFailingOSClient])
async def test_poll_search_pages_should_retry(
    client_cls: Type[_MockFailingClient],
    fail_at: Optional[int],
    max_retries: int,
    failure: Optional[Exception],
    raised: Optional[Type[Exception]],
):
    # Given
    n_hits = 3
    client = client_cls(
        max_retries=max_retries,
        max_retry_wait_s=int(1e-6),
        n_hits=n_hits,
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
