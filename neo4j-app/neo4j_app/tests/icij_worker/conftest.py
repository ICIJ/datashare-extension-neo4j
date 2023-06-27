import asyncio
from typing import AsyncGenerator

import aiohttp
import pytest_asyncio

_RABBITMQ_TEST_PORT = 5673
_RABBITMQ_MANAGEMENT_PORT = 15673
TEST_MANAGEMENT_URL = f"http://localhost:{_RABBITMQ_MANAGEMENT_PORT}"
_DEFAULT_VHOST = "%2F"

_DEFAULT_BROKER_URL = (
    f"amqp://guest:guest@localhost:{_RABBITMQ_TEST_PORT}/{_DEFAULT_VHOST}"
)
_DEFAULT_AUTH = aiohttp.BasicAuth(login="guest", password="guest", encoding="utf-8")


@pytest_asyncio.fixture(scope="session")
async def rabbit_mq_session() -> AsyncGenerator[str, None]:
    try:
        yield _DEFAULT_BROKER_URL
    finally:
        await _wipe_rabbit_mq()


@pytest_asyncio.fixture()
async def rabbit_mq() -> AsyncGenerator[str, None]:
    try:
        yield _DEFAULT_BROKER_URL
    finally:
        await _wipe_rabbit_mq()


def test_management_url(url: str) -> str:
    return f"{TEST_MANAGEMENT_URL}{url}"


async def _wipe_rabbit_mq():
    async with aiohttp.ClientSession(
        raise_for_status=True, auth=_DEFAULT_AUTH
    ) as session:
        await _delete_all_connections(session)
        tasks = [_delete_all_exchanges(session), _delete_all_queues(session)]
        await asyncio.gather(*tasks)


async def _delete_all_connections(session: aiohttp.ClientSession):
    async with session.get(test_management_url("/api/connections")) as res:
        connections = await res.json()
        tasks = [_delete_connection(session, conn["name"]) for conn in connections]
    await asyncio.gather(*tasks)


async def _delete_connection(session: aiohttp.ClientSession, name: str):
    async with session.delete(test_management_url(f"/api/connections/{name}")):
        pass


async def _delete_all_exchanges(session: aiohttp.ClientSession):
    url = f"/api/exchanges/{_DEFAULT_VHOST}"
    async with session.get(test_management_url(url)) as res:
        exchanges = list(await res.json())
        exchanges = (
            ex for ex in exchanges if ex["user_who_performed_action"] == "guest"
        )
        tasks = [_delete_exchange(session, ex["name"]) for ex in exchanges]
    await asyncio.gather(*tasks)


async def _delete_exchange(session: aiohttp.ClientSession, name: str):
    url = f"/api/exchanges/{_DEFAULT_VHOST}/{name}"
    async with session.delete(test_management_url(url)):
        pass


async def _delete_all_queues(session: aiohttp.ClientSession):
    url = f"/api/queues/{_DEFAULT_VHOST}"
    async with session.get(test_management_url(url)) as res:
        queues = await res.json()
    tasks = [_delete_queue(session, q["name"]) for q in queues]
    await asyncio.gather(*tasks)


async def _delete_queue(session: aiohttp.ClientSession, name: str):
    url = f"/api/queues/{_DEFAULT_VHOST}/{name}"
    async with session.delete(test_management_url(url)) as res:
        res.raise_for_status()
