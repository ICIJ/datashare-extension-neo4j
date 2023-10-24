from __future__ import annotations

import configparser
import importlib
import logging
import sys
from configparser import ConfigParser
from enum import Enum, unique
from logging.handlers import SysLogHandler
from typing import Callable, Dict, List, Optional, TextIO, Tuple, Type, Union

import neo4j
from pydantic import Field, validator

from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.elasticsearch.client import ESClient, OSClient
from neo4j_app.core.neo4j.projects import is_enterprise
from neo4j_app.core.utils.logging import (
    DATE_FMT,
    STREAM_HANDLER_FMT,
    STREAM_HANDLER_FMT_WITH_WORKER_ID,
    WorkerIdFilter,
)
from neo4j_app.core.utils.pydantic import (
    BaseICIJModel,
    IgnoreExtraModel,
    LowerCamelCaseModel,
    safe_copy,
)

_SYSLOG_MODEL_SPLIT_CHAR = "@"
_SYSLOG_FMT = f"%(name)s{_SYSLOG_MODEL_SPLIT_CHAR}%(message)s"


@unique
class WorkerType(str, Enum):
    MOCK = "MOCK"
    NEO4J = "NEO4J"

    @property
    def as_worker_cls(self) -> Type["Worker"]:
        if self is WorkerType.NEO4J:
            from neo4j_app.icij_worker import Neo4jAsyncWorker

            return Neo4jAsyncWorker
        if self is WorkerType.MOCK:
            from neo4j_app.tests.icij_worker.conftest import MockWorker

            return MockWorker
        raise NotImplementedError(f"as_worker_cls not implemented for {self}")


def _es_version() -> str:
    import elasticsearch

    return ".".join(str(num) for num in elasticsearch.__version__)


class AppConfig(LowerCamelCaseModel, IgnoreExtraModel):
    doc_app_name: str = "🕸 neo4j app"
    elasticsearch_address: str = "http://127.0.0.1:9200"
    elasticsearch_version: str = Field(default_factory=_es_version, const=True)
    es_doc_type_field: str = Field(alias="docTypeField", default="type")
    es_default_page_size: int = 1000
    es_max_concurrency: int = 5
    es_max_retries: int = 0
    es_max_retry_wait_s: Union[int, float] = 60
    es_timeout_s: Union[int, float] = 60 * 5
    es_keep_alive: str = "1m"
    force_migrations: bool = False
    neo4j_app_async_app: str = "neo4j_app.tasks.app"
    neo4j_app_async_dependencies: Optional[str] = "neo4j_app.tasks.WORKER_LIFESPAN_DEPS"
    neo4j_app_host: str = "127.0.0.1"
    neo4j_app_log_level: str = "INFO"
    neo4j_app_max_records_in_memory: int = int(1e6)
    neo4j_app_migration_timeout_s: float = 60 * 5
    neo4j_app_migration_throttle_s: float = 1
    neo4j_app_n_async_workers: int = 1
    neo4j_app_name: str = "neo4j app"
    neo4j_app_port: int = 8080
    neo4j_app_syslog_facility: Optional[str] = None
    neo4j_app_task_queue_size: int = 2
    neo4j_app_task_queue_poll_interval_s: int = 1.0
    neo4j_app_cancelled_task_refresh_interval_s: int = 2
    neo4j_app_uses_opensearch: bool = False
    neo4j_app_worker_type: WorkerType = WorkerType.NEO4J
    neo4j_concurrency: int = 2
    neo4j_connection_timeout: float = 5.0
    neo4j_host: str = "127.0.0.1"
    neo4j_import_batch_size: int = int(5e5)
    neo4j_password: Optional[str] = None
    neo4j_port: int = 7687
    neo4j_transaction_batch_size = 50000
    neo4j_user: Optional[str] = None
    # Other supported schemes are neo4j+ssc, neo4j+s, bolt, bolt+ssc, bolt+s
    neo4j_uri_scheme: str = "neo4j"
    supports_neo4j_enterprise: Optional[bool] = None
    test: bool = False

    # Ugly but hard to do differently if we want to avoid to retrieve the config on a
    # per request basis using FastApi dependencies...
    _global: Optional[AppConfig] = None

    @validator("neo4j_import_batch_size")
    def neo4j_import_batch_size_must_be_less_than_max_records_in_memory(
        # pylint: disable=no-self-argument
        cls,
        v,
        values,
    ):
        max_records = values["neo4j_app_max_records_in_memory"]
        if v > max_records:
            raise ValueError(
                "neo4j_import_batch_size must be <= neo4j_app_max_records_in_memory"
            )
        return v

    @validator("neo4j_user")
    def neo4j_user_and_password_xor(cls, v, values):  # pylint: disable=no-self-argument
        password = values.get("neo4j_password")
        if bool(password) != bool(v):
            raise ValueError("neo4j authentication is missing user or password")
        return v

    @classmethod
    def from_java_properties(cls, file: TextIO, **kwargs) -> AppConfig:
        parser = ConfigParser(
            allow_no_value=True,
            strict=True,
            # TODO: check this one
            empty_lines_in_values=True,
            interpolation=None,
        )
        # Let's avoid lower-casing the keys
        parser.optionxform = str
        # Config need a section, let's fake one
        section_name = configparser.DEFAULTSECT
        section_str = f"""[{section_name}]
    {file.read()}
    """
        parser.read_string(section_str)
        config_dict = dict(parser[section_name].items())
        config_dict.update(kwargs)
        config_dict = _sanitize_values(config_dict)
        config = AppConfig.parse_obj(config_dict.items())
        return config

    @classmethod
    def set_config_globally(cls, value: AppConfig):
        if cls._global is not None:
            raise ValueError("Can't set config globally twice")
        cls._global = value

    def to_uvicorn(self) -> UviCornModel:
        return UviCornModel(port=self.neo4j_app_port)

    @property
    def neo4j_uri(self) -> str:
        return f"{self.neo4j_uri_scheme}://{self.neo4j_host}:{self.neo4j_port}"

    def to_neo4j_driver(self) -> neo4j.AsyncDriver:
        auth = None
        if self.neo4j_password:
            # TODO: add support for expiring and auto renew auth:
            #  https://neo4j.com/docs/api/python-driver/current/api.html
            #  #neo4j.auth_management.AuthManagers.expiration_based
            auth = neo4j.basic_auth(self.neo4j_user, self.neo4j_password)
        driver = neo4j.AsyncGraphDatabase.driver(
            self.neo4j_uri,
            connection_timeout=self.neo4j_connection_timeout,
            connection_acquisition_timeout=self.neo4j_connection_timeout,
            max_transaction_retry_time=self.neo4j_connection_timeout,
            auth=auth,
        )
        return driver

    def to_es_client(self) -> ESClientABC:
        client_cls = OSClient if self.neo4j_app_uses_opensearch else ESClient
        client = client_cls(
            hosts=[self.elasticsearch_address],
            pagination=self.es_default_page_size,
            max_concurrency=self.es_max_concurrency,
            timeout=self.es_timeout_s,
            max_retries=self.es_max_retries,
            max_retry_wait_s=self.es_max_retry_wait_s,
        )
        return client

    def to_worker_cls(self) -> Type["Worker"]:
        return WorkerType[self.neo4j_app_worker_type].as_worker_cls

    async def with_neo4j_support(self) -> AppConfig:
        async with self.to_neo4j_driver() as neo4j_driver:  # pylint: disable=not-async-context-manager
            support = await is_enterprise(neo4j_driver)
        copied = safe_copy(self, update={"supports_neo4j_enterprise": support})
        return copied

    def setup_loggers(self, worker_id: Optional[str] = None):
        import neo4j_app
        import uvicorn
        import elasticsearch

        loggers = [neo4j_app.__name__, uvicorn.__name__, elasticsearch.__name__]
        force_info = {elasticsearch.__name__}
        try:
            import opensearchpy

            loggers.append(opensearchpy.__name__)
            force_info.add(opensearchpy.__name__)
        except ImportError:
            pass
        worker_id_filter = None
        if worker_id is not None:
            worker_id_filter = WorkerIdFilter(worker_id)
        handlers = self._handlers(worker_id_filter)
        for logger in loggers:
            logger = logging.getLogger(logger)
            level = getattr(logging, self.neo4j_app_log_level)
            if logger.name in force_info:
                level = max(logging.INFO, level)
            logger.setLevel(level)
            logger.handlers = []
            for handler in handlers:
                logger.addHandler(handler)

    def _handlers(
        self, worker_id_filter: Optional[logging.Filter]
    ) -> List[logging.Handler]:
        stream_handler = logging.StreamHandler(sys.stderr)
        if worker_id_filter is not None:
            fmt = STREAM_HANDLER_FMT_WITH_WORKER_ID
        else:
            fmt = STREAM_HANDLER_FMT
        stream_handler.setFormatter(logging.Formatter(fmt, DATE_FMT))
        handlers = [stream_handler]
        if self.neo4j_app_syslog_facility is not None:
            syslog_handler = SysLogHandler(
                facility=self._neo4j_app_syslog_facility_int,
            )
            syslog_handler.setFormatter(logging.Formatter(_SYSLOG_FMT))
            handlers.append(syslog_handler)
        for handler in handlers:
            if worker_id_filter is not None:
                handler.addFilter(worker_id_filter)
            handler.setLevel(self.neo4j_app_log_level)
        return handlers

    @property
    def _neo4j_app_syslog_facility_int(self) -> int:
        try:
            return getattr(
                SysLogHandler, f"LOG_{self.neo4j_app_syslog_facility.upper()}"
            )
        except AttributeError as e:
            msg = f"Invalid syslog facility {self.neo4j_app_syslog_facility}"
            raise ValueError(msg) from e

    def to_async_app(self):
        app_path = self.neo4j_app_async_app.split(".")
        module, app_name = app_path[:-1], app_path[-1]
        module = ".".join(module)
        module = importlib.import_module(module)
        app = getattr(module, app_name)
        app.config = self
        return app

    def to_async_deps(self) -> List[Tuple[Callable, Callable]]:
        deps_path = self.neo4j_app_async_dependencies
        if deps_path is None:
            return []
        deps_path = deps_path.split(".")
        module, app_name = deps_path[:-1], deps_path[-1]
        module = ".".join(module)
        module = importlib.import_module(module)
        deps = getattr(module, app_name)
        return deps


class UviCornModel(BaseICIJModel):
    host: str = Field(default="127.0.0.1", const=True)
    port: int


def _sanitize_values(java_config: Dict[str, str]) -> Dict[str, str]:
    return {
        k: v.replace("\\", "") if isinstance(v, str) else v
        for k, v in java_config.items()
    }
