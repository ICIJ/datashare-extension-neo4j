from __future__ import annotations

import configparser
import functools
from configparser import ConfigParser
from typing import Dict, List, Optional, TextIO

import neo4j
from pydantic import Field

from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.utils.pydantic import (
    BaseICIJModel,
    IgnoreExtraModel,
    LowerCamelCaseModel,
)


class AppConfig(LowerCamelCaseModel, IgnoreExtraModel):
    doc_app_name: str = "🕸 neo4j app"
    es_doc_type_field: str = Field(alias="docTypeField", default="type")
    es_host: str = "127.0.0.1"
    es_port: int = 9200
    es_scroll: str = "1m"
    es_scroll_size: int = 1e4
    neo4j_app_host: str = "127.0.0.1"
    neo4j_app_log_level: str = "INFO"
    neo4j_app_name: str = "neo4j app"
    neo4j_app_port: int = 8080
    neo4j_host: int = "127.0.0.1"
    neo4j_import_dir: str
    neo4j_port: int = 7687
    neo4j_project: str

    # Ugly but hard to do differently if we want to avoid to retrieve the config on a
    # per request basis using FastApi dependencies...
    _global: Optional[AppConfig] = None

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
        return UviCornModel(
            host=self.neo4j_app_host,
            port=self.neo4j_app_port,
            log_level=self.neo4j_app_log_level,
        )

    @property
    def neo4j_uri(self) -> str:
        return f"neo4j://{self.neo4j_host}:{self.neo4j_port}"

    @property
    def es_hosts(self) -> List[Dict]:
        return [{"host": self.es_host, "port": self.es_port}]

    def to_neo4j_driver(self) -> neo4j.AsyncDriver:
        # TODO: forward the creds and the rest of the config...
        driver = neo4j.AsyncGraphDatabase.driver(self.neo4j_uri)
        return driver

    def to_es_client(self) -> ESClient:
        # TODO: read the index name in a secure manner...
        client = ESClient(project_index=self.neo4j_project, hosts=self.es_hosts)
        return client

    @classmethod
    def from_java_properties(cls, file: TextIO) -> AppConfig:
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
        config = AppConfig.parse_obj(config_dict.items())
        return config


class UviCornModel(BaseICIJModel):
    host: str
    port: int
    log_level: str = "INFO"
