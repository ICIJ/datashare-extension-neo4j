import io

import pytest

from neo4j_app.core import AppConfig, UviCornModel


def test_should_support_alias():
    # When
    neo4j_app_name = "test_name"
    config = AppConfig(
        neo4j_app_name=neo4j_app_name,
        neo4j_import_dir="import-dir",
        neo4j_project="test-project",
    )

    # Then
    assert config.neo4j_app_name == neo4j_app_name


@pytest.mark.parametrize(
    "config,expected_config",
    [
        (
            """neo4jProject=test-project
neo4jImportDir=import-dir
""",
            AppConfig(
                neo4j_app_host="127.0.0.1",
                neo4j_app_port=8080,
                neo4j_import_dir="import-dir",
                neo4j_project="test-project",
                elasticsearch_address="http://127.0.0.1:9200",
            ),
        ),
        (
            """neo4jProject=test-project
neo4jImportDir=import-dir
neo4jAppHost=this-the-neo4j-app
neo4jAppPort=3333
elasticsearchAddress=http://elasticsearch:9222
someExtraInfo=useless
""",
            AppConfig(
                neo4j_app_host="this-the-neo4j-app",
                neo4j_app_port=3333,
                neo4j_import_dir="import-dir",
                neo4j_project="test-project",
                elasticsearch_address="http://elasticsearch:9222",
            ),
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
            AppConfig(
                neo4j_app_host="127.0.0.1",
                neo4j_app_port=8888,
                neo4j_import_dir="import-dir",
                neo4j_project="test-project",
            ),
            UviCornModel(host="127.0.0.1", port=8888),
        ),
        (
            AppConfig(
                neo4j_app_log_level="DEBUG",
                neo4j_import_dir="import-dir",
                neo4j_project="test-project",
            ),
            UviCornModel(host="127.0.0.1", port=8080),
        ),
    ],
)
def test_to_uvicorn(config: AppConfig, expected_uvicorn_config: UviCornModel):
    # When
    uvicorn_config = config.to_uvicorn()

    # Then
    assert uvicorn_config == expected_uvicorn_config


def test_should_parse_elasticsearch_address():
    # Given
    config = AppConfig(
        elasticsearch_address="http://elasticsearch:9222",
        neo4j_import_dir="import-dir",
        neo4j_project="test-project",
    )
    # Then
    assert config.es_host == "elasticsearch"
    assert config.es_port == 9222
    assert config.es_hosts == [{"host": "elasticsearch", "port": 9222}]
