from typing import AsyncGenerator

import neo4j
from fastapi import Depends

from neo4j_app.core import AppConfig
from neo4j_app.core.elasticsearch import ESClientABC


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


async def es_client_dep(
    config: AppConfig = Depends(get_global_config_dep),
) -> AsyncGenerator[ESClientABC, None]:
    async with config.to_es_client() as client:
        yield client
