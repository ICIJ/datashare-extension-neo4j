from __future__ import annotations

import configparser
from configparser import ConfigParser
from typing import ClassVar, Dict, List, Optional, TextIO, Union

import elasticsearch
import neo4j
import uvicorn
from icij_common.neo4j.projects import is_enterprise, supports_parallel_runtime
from icij_common.pydantic_utils import (
    IgnoreExtraModel,
    LowerCamelCaseModel,
    NoEnumModel,
    safe_copy,
)
from icij_worker.utils.logging_ import LogWithWorkerIDMixin
from pydantic import Field, validator

import neo4j_app
from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.elasticsearch.client import ESClient, OSClient

_ALL_LOGGERS = [neo4j_app.__name__, uvicorn.__name__, elasticsearch.__name__]
_WARNING_LOGGERS = [elasticsearch.__name__]


def _es_version() -> str:
    return ".".join(str(num) for num in elasticsearch.__version__)


class AppConfig(
    LogWithWorkerIDMixin, LowerCamelCaseModel, IgnoreExtraModel, NoEnumModel
):
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
    force_warning_loggers: ClassVar[List[str]] = _WARNING_LOGGERS
    log_in_json: ClassVar[bool] = Field(default=False, alias="neo4jAppLogInJson")
    log_level: ClassVar[str] = Field(default="INFO", alias="neo4jAppLogLevel")
    loggers: ClassVar[List[str]] = Field(_ALL_LOGGERS, const=True)
    neo4j_app_log_level: str = "INFO"
    neo4j_app_cancelled_tasks_refresh_interval_s: int = 2
    neo4j_app_max_dumped_documents: Optional[int] = None
    neo4j_app_max_records_in_memory: int = int(1e6)
    neo4j_app_migration_timeout_s: float = 60 * 5
    neo4j_app_migration_throttle_s: float = 1
    neo4j_app_n_async_workers: int = 1
    neo4j_app_name: str = "neo4j app"
    neo4j_app_port: int = 8080
    neo4j_app_task_queue_poll_interval_s: int = 1.0
    neo4j_app_uses_opensearch: bool = False
    neo4j_concurrency: int = 2
    neo4j_connection_timeout: float = 5.0
    neo4j_host: str = "127.0.0.1"
    neo4j_import_batch_size: int = int(5e5)
    neo4j_export_batch_size: int = int(1e3)
    neo4j_password: Optional[str] = None
    neo4j_port: int = 7687
    neo4j_transaction_batch_size = 50000
    neo4j_user: Optional[str] = None
    # Other supported schemes are neo4j+ssc, neo4j+s, bolt, bolt+ssc, bolt+s
    neo4j_uri_scheme: str = "bolt"
    supports_neo4j_enterprise: Optional[bool] = None
    supports_neo4j_parallel_runtime: Optional[bool] = None

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
    def _get_config_parser(cls) -> ConfigParser:
        parser = ConfigParser(
            allow_no_value=True,
            strict=True,
            # TODO: check this one
            empty_lines_in_values=True,
            interpolation=None,
        )
        # Let's avoid lower-casing the keys
        parser.optionxform = str
        return parser

    @classmethod
    def from_java_properties(cls, file: TextIO, **kwargs) -> AppConfig:
        parser = cls._get_config_parser()
        # Config need a section, let's fake one
        section_name = configparser.DEFAULTSECT
        section_str = f"""[{section_name}]
        {file.read()}
        """
        parser.read_string(section_str)
        config_dict = dict(parser[section_name].items())
        config_dict.update(kwargs)
        config_dict = _sanitize_values(config_dict)
        config = cls.parse_obj(config_dict.items())
        return config

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
            keep_alive=self.es_keep_alive,
            timeout=self.es_timeout_s,
            max_retries=self.es_max_retries,
            max_retry_wait_s=self.es_max_retry_wait_s,
        )
        return client

    async def with_neo4j_support(self) -> AppConfig:
        async with (  # pylint: disable=not-async-context-manager
            self.to_neo4j_driver() as neo4j_driver
        ):
            enterprise_support = await is_enterprise(neo4j_driver)
            parallel_support = await supports_parallel_runtime(neo4j_driver)
        copied = safe_copy(
            self,
            update={
                "supports_neo4j_enterprise": enterprise_support,
                "supports_neo4j_parallel_runtime": parallel_support,
            },
        )
        return copied


def _sanitize_values(java_config: Dict[str, str]) -> Dict[str, str]:
    return {
        k: v.replace("\\", "") if isinstance(v, str) else v
        for k, v in java_config.items()
    }
