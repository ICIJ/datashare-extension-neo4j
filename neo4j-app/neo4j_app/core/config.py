from __future__ import annotations

import configparser
import functools
import re
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
    elasticsearch_address: str = "http://127.0.0.1:9200"
    es_scroll: str = "1m"
    es_scroll_size: int = 1e4
    neo4j_app_host: str = "127.0.0.1"
    neo4j_app_log_level: str = "info"
    neo4j_app_name: str = "neo4j app"
    neo4j_app_port: int = 8080
    neo4j_host: str = "127.0.0.1"
    neo4j_import_dir: str
    neo4j_import_prefix: Optional[str] = None
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
            port=self.neo4j_app_port,
            log_level=self.neo4j_app_log_level,
        )

    @property
    def neo4j_uri(self) -> str:
        return f"neo4j://{self.neo4j_host}:{self.neo4j_port}"

    @functools.cached_property
    def es_host(self) -> str:
        try:
            host = self._es_address_match.group("host")
        except IndexError as e:
            msg = f"Couldn't find host name in {self.elasticsearch_address}"
            raise ValueError(msg) from e
        if isinstance(host, tuple):
            msg = f"Found several potential hosts in {self.elasticsearch_address}"
            raise ValueError(msg)
        return host

    @functools.cached_property
    def es_port(self) -> int:
        try:
            port = self._es_address_match.group("port")
        except IndexError as e:
            msg = f"Couldn't find port name in {self.elasticsearch_address}"
            raise ValueError(msg) from e
        if isinstance(port, tuple):
            msg = f"Found several potential ports in {self.elasticsearch_address}"
            raise ValueError(msg)
        return int(port)

    @functools.cached_property
    def es_hosts(self) -> List[Dict]:
        return [{"host": self.es_host, "port": self.es_port}]

    def to_neo4j_driver(self) -> neo4j.AsyncDriver:
        # TODO: forward the creds and the rest of the config...
        driver = neo4j.AsyncGraphDatabase.driver(self.neo4j_uri)
        return driver

    def to_es_client(self) -> ESClient:
        # TODO: read the index name in a secure manner...
        client = ESClient(
            project_index=self.neo4j_project,
            hosts=[self.es_host],
            port=self.es_port,
        )
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
        config_dict = _sanitize_values(config_dict)
        config = AppConfig.parse_obj(config_dict.items())
        return config

    @functools.cached_property
    def _es_address_match(self) -> re.Match:
        # It's acceptable not to pre-compile the regex here, it will only be called once
        match = re.match(
            r"^.*://(?P<host>.*):(?P<port>\d{4})$", self.elasticsearch_address
        )
        if match is None:
            raise ValueError(
                f"Ill formatted elasticsearch address: {self.elasticsearch_address}"
            )
        return match


class UviCornModel(BaseICIJModel):
    host: str = Field(default="127.0.0.1", const=True)
    port: int
    log_level: str = "INFO"


def _sanitize_values(java_config: Dict[str, str]) -> Dict[str, str]:
    return {
        k: v.replace("\\", "") if isinstance(v, str) else v
        for k, v in java_config.items()
    }
