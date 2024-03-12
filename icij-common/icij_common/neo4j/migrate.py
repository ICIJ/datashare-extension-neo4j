from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Coroutine
from datetime import datetime
from distutils.version import StrictVersion
from enum import Enum, unique
from inspect import signature
from typing import Any, Callable, List, Optional, Sequence, Union

import neo4j
from neo4j.exceptions import ConstraintError

from icij_common.neo4j.constants import (
    MIGRATION_COMPLETED,
    MIGRATION_LABEL,
    MIGRATION_NODE,
    MIGRATION_PROJECT,
    MIGRATION_STARTED,
    MIGRATION_STATUS,
    MIGRATION_VERSION,
)
from icij_common.pydantic_utils import NoEnumModel
from .projects import (
    Project,
    create_project_db,
    create_project_tx,
    project_db_session,
    projects_tx,
    registry_db_session,
)

logger = logging.getLogger(__name__)

TransactionFn = Callable[[neo4j.AsyncTransaction], Coroutine]
ExplicitTransactionFn = Callable[[neo4j.Session], Coroutine]
MigrationFn = Union[TransactionFn, ExplicitTransactionFn]

_MIGRATION_TIMEOUT_MSG = """Migration timeout expired !
Please check that a migration is indeed in progress. If the application is in a \
deadlock restart it forcing the migration index cleanup."""


class MigrationError(RuntimeError):
    pass


@unique
class MigrationStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"


class MigrationVersion(StrictVersion):
    @classmethod
    def __get_validators__(cls):
        def validator(v: Any) -> MigrationVersion:
            if isinstance(v, (str, MigrationVersion)):
                return MigrationVersion(v)
            raise ValueError(
                f"Must be a {MigrationVersion.__name__} or a {str.__name__}, "
                f"found {type(v)}"
            )

        yield validator


class _BaseMigration(NoEnumModel):
    version: MigrationVersion
    label: str
    status: MigrationStatus = MigrationStatus.IN_PROGRESS


class Neo4jMigration(_BaseMigration):
    # It would have been cleaner to create
    # (p:_Project)-[:_RUNS { id: p.name + m.version }]->(m:_Migration)
    # relationships. However, neo4j < 5.7 doesn't support unique constraint on
    # relationships properties which prevents from implementing the locking mechanism
    # properly. We hence enforce unique constraint on
    # (_Migration.version, _Migration.project)
    project: str
    started: datetime
    completed: Optional[datetime] = None
    status: MigrationStatus = MigrationStatus.IN_PROGRESS

    @classmethod
    def from_neo4j(cls, record: neo4j.Record, key="migration") -> Neo4jMigration:
        migration = dict(record.value(key))
        if "started" in migration:
            migration["started"] = migration["started"].to_native()
        if "completed" in migration:
            migration["completed"] = migration["completed"].to_native()
        return Neo4jMigration(**migration)


class Migration(_BaseMigration):
    migration_fn: MigrationFn


MigrationRegistry: Sequence[Migration]


async def _migrate_with_lock(
    *,
    project_session: neo4j.AsyncSession,
    registry_session: neo4j.AsyncSession,
    project: str,
    migration: Migration,
):
    # Note: all migrations.py should be carefully tested otherwise they will lock
    # the DB...

    # Lock the DB first, raising in case a migration already exists
    logger.debug("Trying to run migration to %s...", migration.label)
    await registry_session.execute_write(
        create_migration_tx,
        project=project,
        migration_version=str(migration.version),
        migration_label=migration.label,
    )
    # Then run to migration
    logger.debug("Acquired write lock to %s !", migration.label)
    sig = signature(migration.migration_fn)
    first_param = list(sig.parameters)[0]
    if first_param == "tx":
        await project_session.execute_write(migration.migration_fn)
    elif first_param == "sess":
        await migration.migration_fn(project_session)
    else:
        raise ValueError(f"Invalid migration function: {migration.migration_fn}")
    # Finally free the lock
    await registry_session.execute_write(
        complete_migration_tx,
        project=project,
        migration_version=str(migration.version),
    )
    logger.debug("Marked %s as complete !", migration.label)


async def create_migration_tx(
    tx: neo4j.AsyncTransaction,
    *,
    project: str,
    migration_version: str,
    migration_label: str,
) -> Neo4jMigration:
    query = f"""CREATE (m:{MIGRATION_NODE} {{
    {MIGRATION_PROJECT}: $project,
    {MIGRATION_LABEL}: $label,
    {MIGRATION_VERSION}: $version,
    {MIGRATION_STATUS}: $status,
    {MIGRATION_STARTED}: $started
}})
RETURN m as migration"""
    res = await tx.run(
        query,
        label=migration_label,
        version=migration_version,
        project=project,
        status=MigrationStatus.IN_PROGRESS.value,
        started=datetime.now(),
    )
    migration = await res.single()
    if migration is None:
        raise ValueError(f"Couldn't find migration {migration_version} for {project}")
    migration = Neo4jMigration.from_neo4j(migration)
    return migration


async def complete_migration_tx(
    tx: neo4j.AsyncTransaction, *, project: str, migration_version: str
) -> Neo4jMigration:
    query = f"""MATCH (m:{MIGRATION_NODE} {{
        {MIGRATION_VERSION}: $version,
        {MIGRATION_PROJECT}: $project
     }})
SET m += {{ {MIGRATION_STATUS}: $status, {MIGRATION_COMPLETED}: $completed }} 
RETURN m as migration"""
    res = await tx.run(
        query,
        version=migration_version,
        project=project,
        status=MigrationStatus.DONE.value,
        completed=datetime.now(),
    )
    migration = await res.single()
    migration = Neo4jMigration.from_neo4j(migration)
    return migration


async def project_migrations_tx(
    tx: neo4j.AsyncTransaction, project: str
) -> List[Neo4jMigration]:
    query = f"""MATCH (m:{MIGRATION_NODE} {{ {MIGRATION_PROJECT}: $project }})
RETURN m as migration
"""
    res = await tx.run(query, project=project)
    migrations = [Neo4jMigration.from_neo4j(rec) async for rec in res]
    return migrations


async def delete_all_migrations(driver: neo4j.AsyncDriver):
    query = f"""MATCH (m:{MIGRATION_NODE})
DETACH DELETE m"""
    async with registry_db_session(driver) as sess:
        await sess.run(query)


async def retrieve_projects(neo4j_driver: neo4j.AsyncDriver) -> List[Project]:
    async with registry_db_session(neo4j_driver) as sess:
        projects = await sess.execute_read(projects_tx)
    return projects


async def migrate_db_schemas(
    neo4j_driver: neo4j.AsyncDriver,
    registry: MigrationRegistry,
    *,
    timeout_s: float,
    throttle_s: float,
):
    projects = await retrieve_projects(neo4j_driver)
    tasks = [
        migrate_project_db_schema(
            neo4j_driver,
            registry,
            project=p.name,
            timeout_s=timeout_s,
            throttle_s=throttle_s,
        )
        for p in projects
    ]
    await asyncio.gather(*tasks)


async def migrate_project_db_schema(
    neo4j_driver: neo4j.AsyncDriver,
    registry: MigrationRegistry,
    project: str,
    *,
    timeout_s: float,
    throttle_s: float,
):
    logger.info("Migrating project %s", project)
    start = time.monotonic()
    if not registry:
        return
    todo = sorted(registry, key=lambda m: m.version)
    async with registry_db_session(neo4j_driver) as registry_sess:
        async with project_db_session(neo4j_driver, project=project) as project_sess:
            while "Waiting for DB to be migrated or for a timeout":
                migrations = await registry_sess.execute_read(
                    project_migrations_tx, project=project
                )
                in_progress = [
                    m for m in migrations if m.status is MigrationStatus.IN_PROGRESS
                ]
                if len(in_progress) > 1:
                    raise MigrationError(
                        f"Found several migration in progress: {in_progress}"
                    )
                if in_progress:
                    logger.info(
                        "Found that %s is in progress, waiting for %s seconds...",
                        in_progress[0].label,
                        throttle_s,
                    )
                    await asyncio.sleep(throttle_s)
                else:
                    done = [m for m in migrations if m.status is MigrationStatus.DONE]
                    if done:
                        current_version = max((m.version for m in done))
                        todo = [m for m in todo if m.version > current_version]
                    if not todo:
                        break
                    try:
                        await _migrate_with_lock(
                            project_session=project_sess,
                            registry_session=registry_sess,
                            project=project,
                            migration=todo[0],
                        )
                        todo = todo[1:]
                        continue
                    except ConstraintError:
                        logger.info(
                            "Migration %s has just started somewhere else, "
                            " waiting for %s seconds...",
                            todo[0].label,
                            throttle_s,
                        )
                        await asyncio.sleep(throttle_s)
                elapsed = time.monotonic() - start
                if elapsed > timeout_s:
                    logger.error(_MIGRATION_TIMEOUT_MSG)
                    raise MigrationError(_MIGRATION_TIMEOUT_MSG)
                continue


async def init_project(
    neo4j_driver: neo4j.AsyncDriver,
    name: str,
    registry: MigrationRegistry,
    *,
    timeout_s: float,
    throttle_s: float,
) -> bool:
    # Create project DB
    await create_project_db(neo4j_driver, project=name)

    # Create project
    async with registry_db_session(neo4j_driver) as sess:
        project, already_exists = await sess.execute_write(create_project_tx, name=name)

    # Migrate it
    await migrate_project_db_schema(
        neo4j_driver,
        registry=registry,
        project=project.name,
        timeout_s=timeout_s,
        throttle_s=throttle_s,
    )

    return already_exists
