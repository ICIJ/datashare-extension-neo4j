import io

import pytest

from neo4j_app.core import AppConfig, UviCornModel


def test_should_support_alias():
    # When
    neo4j_app_name = "test_name"
    config = AppConfig(neo4j_app_name=neo4j_app_name)

    # Then
    assert config.neo4j_app_name == neo4j_app_name


@pytest.mark.parametrize(
    "config,expected_config",
    [
        (
            "",
            AppConfig(neo4j_app_host="127.0.0.1", neo4j_app_port=8080),
        ),
        (
            """neo4jAppHost=this-the-neo4j-app
neo4jAppPort=3333
someExtraInfo=useless
""",
            AppConfig(neo4j_app_host="this-the-neo4j-app", neo4j_app_port=3333),
        ),
    ],
)
def test_should_load_from_java(config: str, expected_config: AppConfig):
    # Given
    config_stream = io.StringIO(config)

    # When
    loaded_config = AppConfig.from_java_properties(config_stream)

    # Then
    assert loaded_config == expected_config


@pytest.mark.parametrize(
    "config,expected_uvicorn_config",
    [
        (
            AppConfig(neo4j_app_host="127.0.0.3", neo4j_app_port=8888),
            UviCornModel(host="127.0.0.3", port=8888, log_level="INFO"),
        ),
        (
            AppConfig(neo4j_app_log_level="DEBUG"),
            UviCornModel(host="127.0.0.1", port=8080, log_level="DEBUG"),
        ),
    ],
)
def test_to_uvicorn(config: AppConfig, expected_uvicorn_config: UviCornModel):
    # When
    uvicorn_config = config.to_uvicorn()

    # Then
    assert uvicorn_config == expected_uvicorn_config
