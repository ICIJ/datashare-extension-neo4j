from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Coroutine
from datetime import datetime
from distutils.version import StrictVersion
from enum import Enum, unique
from typing import Any, Callable, List, Optional, Sequence

import neo4j
from neo4j.exceptions import ConstraintError

from neo4j_app.constants import (
    MIGRATION_COMPLETED,
    MIGRATION_LABEL,
    MIGRATION_NODE,
    MIGRATION_STARTED,
    MIGRATION_STATUS,
    MIGRATION_VERSION,
)
from neo4j_app.core.utils.pydantic import NoEnumModel

logger = logging.getLogger(__name__)

MigrationFn = Callable[[neo4j.AsyncTransaction], Coroutine]

_NEO4J_SYSTEM_DB = "system"

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
    version: MigrationVersion
    label: str
    started: datetime
    completed: Optional[datetime] = None

    @classmethod
    def from_neo4j(cls, record: neo4j.Record, key="migration") -> Neo4jMigration:
        migration = dict(record[key])
        if "started" in migration:
            migration["started"] = migration["started"].to_native()
        if "completed" in migration:
            migration["completed"] = migration["completed"].to_native()
        return Neo4jMigration(**migration)


class Migration(_BaseMigration):
    migration_fn: MigrationFn


MigrationRegistry: Sequence[Migration]


async def _migration_wrapper(neo4j_session: neo4j.AsyncSession, migration: Migration):
    # Note: all migrations.py should be carefully test otherwise they will lock
    # the DB...

    # Lock the DB first, raising in case a migration already exists
    logger.debug("Trying to run migration %s...", migration.label)
    await neo4j_session.execute_write(
        create_migration_tx,
        migration_version=str(migration.version),
        migration_label=migration.label,
    )
    # Then run to migration
    logger.debug("Acquired write lock for %s !", migration.label)
    await neo4j_session.execute_write(migration.migration_fn)
    # Finally free the lock
    await neo4j_session.execute_write(
        complete_migration_tx, version=str(migration.version)
    )
    logger.debug("Marked %s as complete !", migration.label)


async def create_migration_tx(
    tx: neo4j.AsyncTransaction,
    *,
    migration_version: str,
    migration_label: str,
) -> Neo4jMigration:
    query = f"""CREATE (m:{MIGRATION_NODE} {{
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
        status=MigrationStatus.IN_PROGRESS.value,
        started=datetime.now(),
    )
    m = await res.single()
    m = Neo4jMigration.from_neo4j(m, key="migration")
    return m


async def complete_migration_tx(
    tx: neo4j.AsyncTransaction, version: str
) -> Neo4jMigration:
    query = f"""MATCH (m:{MIGRATION_NODE} {{ {MIGRATION_VERSION}: $version }})
SET m += {{ {MIGRATION_STATUS}: $status, {MIGRATION_COMPLETED}: $completed }} 
RETURN m as migration"""
    res = await tx.run(
        query,
        version=version,
        status=MigrationStatus.DONE.value,
        completed=datetime.now(),
    )
    m = await res.single()
    m = Neo4jMigration.from_neo4j(m, key="migration")
    return m


async def migrations_tx(tx: neo4j.AsyncTransaction) -> List[Neo4jMigration]:
    query = f"""MATCH (m:{MIGRATION_NODE})
RETURN m as migration
"""
    res = await tx.run(query)
    migrations = [Neo4jMigration.from_neo4j(rec, key="migration") async for rec in res]
    return migrations


async def delete_all_migrations(driver: neo4j.AsyncDriver):
    query = f"""MATCH (m:{MIGRATION_NODE})
DETACH DELETE m
    """
    await driver.execute_query(query)


async def retrieve_project_dbs(neo4j_driver: neo4j.AsyncDriver) -> List[str]:
    query = f"""SHOW DATABASES YIELD name, currentStatus
WHERE currentStatus = "online" and name <> "{_NEO4J_SYSTEM_DB}"
RETURN name
"""
    records, _, _ = await neo4j_driver.execute_query(query)
    project_dbs = [rec["name"] for rec in records]
    return project_dbs


async def migrate_db_schemas(
    neo4j_driver: neo4j.AsyncDriver,
    registry: MigrationRegistry,
    *,
    timeout_s: float,
    throttle_s: float,
):
    project_dbs = await retrieve_project_dbs(neo4j_driver)
    tasks = [
        migrate_project_db_schema(
            neo4j_driver, registry, db=db, timeout_s=timeout_s, throttle_s=throttle_s
        )
        for db in project_dbs
    ]
    await asyncio.gather(*tasks)


# Retrieve all project DBs


async def migrate_project_db_schema(
    neo4j_driver: neo4j.AsyncDriver,
    registry: MigrationRegistry,
    db: str,
    *,
    timeout_s: float,
    throttle_s: float,
):
    logger.info("Migrating project DB %s", db)
    start = time.monotonic()
    if not registry:
        return
    todo = sorted(registry, key=lambda m: m.version)
    async with neo4j_driver.session(database=db) as sess:
        while "Waiting for DB to be migrated or for a timeout":
            elapsed = time.monotonic() - start
            if elapsed > timeout_s:
                logger.error(_MIGRATION_TIMEOUT_MSG)
                raise MigrationError(_MIGRATION_TIMEOUT_MSG)
            migrations = await sess.execute_read(migrations_tx)
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
                continue
            done = [m for m in migrations if m.status is MigrationStatus.DONE]
            if done:
                current_version = max((m.version for m in done))
                todo = [m for m in todo if m.version > current_version]
            if not todo:
                break
            try:
                await _migration_wrapper(sess, todo[0])
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
                continue
