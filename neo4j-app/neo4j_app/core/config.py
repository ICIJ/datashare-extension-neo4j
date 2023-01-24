from __future__ import annotations

import configparser
from configparser import ConfigParser
from typing import TextIO

from neo4j_app.utils.pydantic import (
    BaseICIJConfig,
    IgnoreExtraConfig,
    LowerCamelCaseConfig,
)


class AppConfig(LowerCamelCaseConfig, IgnoreExtraConfig):
    neo4j_app_name: str = "neo4j app"
    neo4j_app_host: str = "127.0.0.1"
    neo4j_app_port: int = 8080
    neo4j_app_log_level: str = "INFO"

    def to_uvicorn(self) -> UviCornConfig:
        return UviCornConfig(
            host=self.neo4j_app_host,
            port=self.neo4j_app_port,
            log_level=self.neo4j_app_log_level,
        )

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


class UviCornConfig(BaseICIJConfig):
    host: str
    port: int
    log_level: str = "INFO"
