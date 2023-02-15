from typing import AsyncGenerator

import neo4j
from fastapi import Depends, Request

from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClient


def get_global_config_dep() -> AppConfig:
    return AppConfig.get_global_config()


async def neo4j_driver_dep(
    config: AppConfig = Depends(get_global_config_dep),
) -> AsyncGenerator[neo4j.AsyncDriver, None]:
    driver = config.to_neo4j_driver()
    try:
        yield driver
    finally:
        await driver.close()


async def neo4j_session_dep(
    request: Request,
    driver: neo4j.AsyncNeo4jDriver = Depends(neo4j_driver_dep),
) -> AsyncGenerator[neo4j.AsyncSession, None]:
    async with driver.session() as sess:
        request.state.neo4j_session = sess
        yield sess


async def es_client_dep(
    config: AppConfig = Depends(get_global_config_dep),
) -> AsyncGenerator[ESClient, None]:
    async with config.to_es_client() as client:
        yield client
