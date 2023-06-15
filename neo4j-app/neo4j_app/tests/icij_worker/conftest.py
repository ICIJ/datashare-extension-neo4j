import asyncio
import json

import aiohttp
import pytest

_RABBITMQ_TEST_PORT = 5673
_RABBITMQ_MANAGEMENT_PORT = 15673
_BASE_MANAGEMENT_URL = f"http://localhost:{_RABBITMQ_MANAGEMENT_PORT}"
_DEFAULT_VHOST = "%2F"

_DEFAULT_BROKER_URL = (
    f"amqp://guest:guest@localhost:{_RABBITMQ_TEST_PORT}/{_DEFAULT_VHOST}"
)


@pytest.fixture(scope="session")
def rabbit_mq_session() -> str:
    try:
        yield _DEFAULT_BROKER_URL
    finally:
        # TODO: fix this
        # try:
        _wipe_rabbit_mq()
        # except ClientConnectorError: # The broker is not alive no need to do anything
        #     pass


@pytest.fixture()
def rabbit_mq() -> str:
    try:
        yield _DEFAULT_BROKER_URL
    finally:
        # TODO: fix this
        # try:
        _wipe_rabbit_mq()
        # except ClientConnectorError: # The broker is not alive no need to do anything
        #     pass


def _make_management_url(url: str) -> str:
    return f"{_BASE_MANAGEMENT_URL}{url}"


async def _wipe_rabbit_mq():
    async with aiohttp.ClientSession() as session:
        await _delete_all_connections(session)
        tasks = [_delete_all_exchanges(session), _delete_all_queues(session)]
        await asyncio.gather(*tasks)


async def _delete_all_connections(session: aiohttp.ClientSession):
    async with session.get(_make_management_url("/api/connections")) as res:
        conns = json.loads(await res.json())
    tasks = [_delete_connection(session, conn["name"]) for conn in conns]
    await asyncio.gather(*tasks)


async def _delete_connection(session: aiohttp.ClientSession, name: str):
    async with session.delete(_make_management_url(f"/api/connections/{name}")) as res:
        res.raise_for_status()


async def _delete_all_exchanges(session: aiohttp.ClientSession):
    url = f"/api/exchanges/{_DEFAULT_VHOST}"
    async with session.get(_make_management_url(url)) as res:
        exs = json.loads(await res.json())
    tasks = [_delete_exchange(session, ex["name"]) for ex in exs]
    await asyncio.gather(*tasks)


async def _delete_exchange(session: aiohttp.ClientSession, name: str):
    url = f"/api/exchanges/{_DEFAULT_VHOST}/{name}"
    async with session.delete(_make_management_url(url)) as res:
        res.raise_for_status()


async def _delete_all_queues(session: aiohttp.ClientSession):
    url = f"/api/queues/{_DEFAULT_VHOST}"
    async with session.get(_make_management_url(url)) as res:
        queues = json.loads(await res.json())
    tasks = [_delete_queue(session, q["name"]) for q in queues]
    await asyncio.gather(*tasks)


async def _delete_queue(session: aiohttp.ClientSession, name: str):
    url = f"/api/queues/{_DEFAULT_VHOST}/{name}"
    async with session.delete(_make_management_url(url)) as res:
        res.raise_for_status()
