from __future__ import annotations
import logging
from contextlib import asynccontextmanager
from typing import List, Tuple

import neo4j

from neo4j_app.constants import PROJECT_NAME, PROJECT_NODE, PROJECT_REGISTRY_DB
from neo4j_app.core.utils.pydantic import BaseICIJModel

logger = logging.getLogger(__name__)

NEO4J_COMMUNITY_DB = "neo4j"
_IS_ENTERPRISE = None


class Project(BaseICIJModel):
    name: str

    @classmethod
    def from_neo4j(cls, record: neo4j.Record, key="project") -> Project:
        project = dict(record.value(key))
        return Project(**project)


async def create_project_registry_db(neo4j_driver: neo4j.AsyncDriver):
    async with neo4j_driver:
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
) -> neo4j.AsyncSession:
    db = await project_db(neo4j_driver, project)
    sess_ctx = neo4j_driver.session(database=db)
    async with sess_ctx as sess:
        yield sess


@asynccontextmanager
async def registry_db_session(neo4j_driver: neo4j.AsyncDriver) -> neo4j.AsyncSession:
    session = neo4j_driver.session(database=await project_registry_db(neo4j_driver))
    async with session as sess:
        yield sess


async def is_enterprise(neo4j_driver: neo4j.AsyncDriver) -> bool:
    global _IS_ENTERPRISE
    if _IS_ENTERPRISE is None:
        query = "CALL dbms.components() YIELD edition RETURN edition"
        res, _, _ = await neo4j_driver.execute_query(query)
        _IS_ENTERPRISE = res[0]["edition"] != "community"
    return _IS_ENTERPRISE
