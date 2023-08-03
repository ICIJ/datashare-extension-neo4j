from __future__ import annotations

import configparser
import functools
import logging
import sys
from configparser import ConfigParser
from logging.handlers import SysLogHandler
from typing import Dict, List, Optional, TextIO

import neo4j
from pydantic import Field, validator

from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.elasticsearch.client import ESClient, OSClient
from neo4j_app.core.utils.logging import DATE_FMT, STREAM_HANDLER_FMT
from neo4j_app.core.utils.pydantic import (
    BaseICIJModel,
    IgnoreExtraModel,
    LowerCamelCaseModel,
)

_SYSLOG_MODEL_SPLIT_CHAR = "@"
_SYSLOG_FMT = f"%(name)s{_SYSLOG_MODEL_SPLIT_CHAR}%(message)s"


def _es_version():
    import elasticsearch

    return ".".join(str(num) for num in elasticsearch.__version__)


class AppConfig(LowerCamelCaseModel, IgnoreExtraModel):
    doc_app_name: str = "🕸 neo4j app"
    elasticsearch_address: str = "http://127.0.0.1:9200"
    elasticsearch_version: str = Field(default_factory=_es_version, const=True)
    es_doc_type_field: str = Field(alias="docTypeField", default="type")
    es_default_page_size: int = 1000
    es_max_concurrency: int = 5
    es_timeout: int = "1m"
    es_keep_alive: str = "1m"
    neo4j_app_host: str = "127.0.0.1"
    neo4j_app_log_level: str = "INFO"
    neo4j_app_max_records_in_memory: int = int(1e6)
    neo4j_app_migration_timeout_s: float = 60 * 5
    neo4j_app_migration_throttle_s: float = 1
    neo4j_app_name: str = "neo4j app"
    neo4j_app_port: int = 8080
    neo4j_app_syslog_facility: Optional[str] = None
    neo4j_app_uses_opensearch: bool = False
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
    force_migrations: bool = False

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
    @functools.lru_cache
    def get_global_config(cls) -> AppConfig:
        if cls._global is None:
            raise ValueError("Config was not set globally")
        return cls._global

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
            auth=auth,
        )
        return driver

    # TODO: change this to output ESClientMixin...
    def to_es_client(self) -> ESClientABC:
        client_cls = OSClient if self.neo4j_app_uses_opensearch else ESClient
        # TODO: read the index name in a secure manner...
        client = client_cls(
            hosts=[self.elasticsearch_address],
            pagination=self.es_default_page_size,
            max_concurrency=self.es_max_concurrency,
        )
        return client

    def setup_loggers(self):
        import neo4j_app
        import uvicorn
        import elasticsearch

        loggers = [
            neo4j_app.__name__,
            uvicorn.__name__,
            elasticsearch.__name__,
        ]
        try:
            import opensearchpy

            loggers.append(opensearchpy.__name__)
        except ImportError:
            pass

        for handler in self._handlers:
            handler.setLevel(self.neo4j_app_log_level)

        for logger in loggers:
            logger = logging.getLogger(logger)
            level = getattr(logging, self.neo4j_app_log_level)
            if logger == elasticsearch.__name__:
                level = max(logging.INFO, level)
            logger.setLevel(level)
            logger.handlers = []
            for handler in self._handlers:
                logger.addHandler(handler)

    @functools.cached_property
    def _handlers(self) -> List[logging.Handler]:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(logging.Formatter(STREAM_HANDLER_FMT, DATE_FMT))
        handlers = [stream_handler]
        if self.neo4j_app_syslog_facility is not None:
            syslog_handler = SysLogHandler(
                facility=self._neo4j_app_syslog_facility_int,
            )
            syslog_handler.setFormatter(logging.Formatter(_SYSLOG_FMT))
            handlers.append(syslog_handler)
        return handlers

    @functools.cached_property
    def _neo4j_app_syslog_facility_int(self) -> int:
        try:
            return getattr(
                SysLogHandler, f"LOG_{self.neo4j_app_syslog_facility.upper()}"
            )
        except AttributeError as e:
            msg = f"Invalid syslog facility {self.neo4j_app_syslog_facility}"
            raise ValueError(msg) from e


class UviCornModel(BaseICIJModel):
    host: str = Field(default="127.0.0.1", const=True)
    port: int


def _sanitize_values(java_config: Dict[str, str]) -> Dict[str, str]:
    return {
        k: v.replace("\\", "") if isinstance(v, str) else v
        for k, v in java_config.items()
    }
