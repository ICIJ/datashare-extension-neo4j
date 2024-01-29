from __future__ import annotations

import configparser
import functools
import io
from copy import copy
from typing import Optional, TextIO

from pydantic import Field

from neo4j_app import AppConfig
from neo4j_app.core.utils.pydantic import (
    BaseICIJModel,
)
from neo4j_app.icij_worker import WorkerConfig, WorkerType

_SHARED_WITH_NEO4J_WORKER_CONFIG = [
    "neo4j_connection_timeout",
    "neo4j_host",
    "neo4j_password",
    "neo4j_port",
    "neo4j_uri_scheme",
    "neo4j_user",
]

_SHARED_WITH_NEO4J_WORKER_CONFIG_PREFIXED = [
    "cancelled_tasks_refresh_interval_s",
    "task_queue_poll_interval_s",
    "log_level",
]


class ServiceConfig(AppConfig):
    neo4j_app_async_dependencies: Optional[str] = "neo4j_app.tasks.WORKER_LIFESPAN_DEPS"
    neo4j_app_async_app: str = "neo4j_app.tasks.app"
    neo4j_app_gunicorn_workers: int = 1
    neo4j_app_host: str = "127.0.0.1"
    neo4j_app_n_async_workers: int = 1
    neo4j_app_name: str = "🕸 neo4j app"
    neo4j_app_port: int = 8080
    neo4j_app_task_queue_size: int = 2
    neo4j_app_worker_type: WorkerType = WorkerType.neo4j
    test: bool = False

    @functools.cached_property
    def doc_app_name(self) -> str:
        return self.neo4j_app_name

    def to_worker_config(self, **kwargs) -> WorkerConfig:
        kwargs = copy(kwargs)
        for suffix in _SHARED_WITH_NEO4J_WORKER_CONFIG_PREFIXED:
            kwargs[suffix] = getattr(self, f"neo4j_app_{suffix}")

        if self.test:
            from neo4j_app.tests.icij_worker.conftest import MockWorkerConfig

            return MockWorkerConfig(**kwargs)
        from neo4j_app.icij_worker.worker.neo4j import Neo4jWorkerConfig

        for k in _SHARED_WITH_NEO4J_WORKER_CONFIG:
            if k in kwargs:
                continue
            kwargs[k] = getattr(self, k)
        return Neo4jWorkerConfig(**kwargs)

    def write_java_properties(self, file: TextIO):
        parser = self._get_config_parser()
        parser[configparser.DEFAULTSECT] = dict(
            sorted(self.dict(exclude_unset=True, by_alias=True).items())
        )
        config_str_io = io.StringIO()
        parser.write(config_str_io, space_around_delimiters=False)
        config_str = config_str_io.getvalue()
        # Remove the mandatory default section
        config_str = config_str.replace(f"[{configparser.DEFAULTSECT}]\n", "")
        file.write(config_str)


class UviCornModel(BaseICIJModel):
    host: str = Field(default="127.0.0.1", const=True)
    port: int
