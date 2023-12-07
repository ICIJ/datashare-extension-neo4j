from __future__ import annotations
import logging
from contextlib import asynccontextmanager
from distutils.version import StrictVersion
from typing import AsyncGenerator, List, Optional, Tuple

import neo4j

from neo4j_app.constants import PROJECT_NAME, PROJECT_NODE, PROJECT_REGISTRY_DB
from neo4j_app.core.utils.pydantic import BaseICIJModel

logger = logging.getLogger(__name__)

NEO4J_COMMUNITY_DB = "neo4j"
_IS_ENTERPRISE: Optional[bool] = None
_NEO4J_VERSION: Optional[StrictVersion] = None
_SUPPORTS_PARALLEL: Optional[bool] = None

_COMPONENTS_QUERY = """CALL dbms.components() YIELD versions, edition
RETURN versions, edition"""


class Project(BaseICIJModel):
    name: str

    @classmethod
    def from_neo4j(cls, record: neo4j.Record, key="project") -> Project:
        project = dict(record.value(key))
        return Project(**project)


async def create_project_registry_db(neo4j_driver: neo4j.AsyncDriver):
    if await is_enterprise(neo4j_driver):
        logger.info("Creating project registry DB...")
        query = "CREATE DATABASE $registry_db IF NOT EXISTS"
        await neo4j_driver.execute_query(query, registry_db=PROJECT_REGISTRY_DB)
    else:
        logger.info("Using default db as registry DB !")


async def projects_tx(tx: neo4j.AsyncTransaction) -> List[Project]:
    query = f"MATCH (project:{PROJECT_NODE}) RETURN project"
    res = await tx.run(query)
    projects = [Project.from_neo4j(p) async for p in res]
    return projects


async def create_project_db(neo4j_driver: neo4j.AsyncDriver, project: str):
    if await is_enterprise(neo4j_driver):
        db_name = await project_db(neo4j_driver, project=project)
        query = "CREATE DATABASE $db_name IF NOT EXISTS"
        await neo4j_driver.execute_query(query, db_name=db_name)


async def create_project_tx(
    tx: neo4j.AsyncTransaction, name: str
) -> Tuple[Project, bool]:
    if name == PROJECT_REGISTRY_DB:
        raise ValueError(
            f'Bad luck, name "{PROJECT_REGISTRY_DB}" is reserved for internal use.'
            f" Can't initialize project"
        )
    query = f"""MERGE (project:{PROJECT_NODE} {{ {PROJECT_NAME}: $name }})
RETURN project"""
    res = await tx.run(query, name=name)
    rec = await res.single()
    summary = await res.consume()
    existed = summary.counters.nodes_created == 0
    project = Project.from_neo4j(rec)
    return project, existed


async def project_registry_db(neo4j_driver: neo4j.AsyncDriver) -> str:
    if await is_enterprise(neo4j_driver):
        return PROJECT_REGISTRY_DB
    return NEO4J_COMMUNITY_DB


async def project_db(neo4j_driver: neo4j.AsyncDriver, project: str) -> str:
    if await is_enterprise(neo4j_driver):
        return project
    return NEO4J_COMMUNITY_DB


def project_index(project: str) -> str:
    return project


@asynccontextmanager
async def project_db_session(
    neo4j_driver: neo4j.AsyncDriver, project: str
) -> AsyncGenerator[neo4j.AsyncSession, None]:
    db = await project_db(neo4j_driver, project)
    sess_ctx = neo4j_driver.session(database=db)
    async with sess_ctx as sess:
        yield sess


@asynccontextmanager
async def registry_db_session(neo4j_driver: neo4j.AsyncDriver) -> neo4j.AsyncSession:
    session = neo4j_driver.session(database=await project_registry_db(neo4j_driver))
    async with session as sess:
        yield sess


async def _get_components(neo4j_driver: neo4j.AsyncDriver):
    async with neo4j_driver.session(database=neo4j.SYSTEM_DATABASE) as sess:
        res = await sess.run(_COMPONENTS_QUERY)
        res = await res.single()
    global _IS_ENTERPRISE
    global _NEO4J_VERSION
    _IS_ENTERPRISE = res["edition"] != "community"
    _NEO4J_VERSION = StrictVersion(res["versions"][0])


async def server_version(neo4j_driver: neo4j.AsyncDriver) -> StrictVersion:
    if _NEO4J_VERSION is None:
        await _get_components(neo4j_driver)
    return _NEO4J_VERSION


async def supports_parallel_runtime(neo4j_driver: neo4j.AsyncDriver) -> bool:
    global _SUPPORTS_PARALLEL
    if _SUPPORTS_PARALLEL is None:
        version = await server_version(neo4j_driver)
        _SUPPORTS_PARALLEL = version >= StrictVersion("5.13")
    return _SUPPORTS_PARALLEL


async def is_enterprise(neo4j_driver: neo4j.AsyncDriver) -> bool:
    if _IS_ENTERPRISE is None:
        await _get_components(neo4j_driver)
    return _IS_ENTERPRISE
