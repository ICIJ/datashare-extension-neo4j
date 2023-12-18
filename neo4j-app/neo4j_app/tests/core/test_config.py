import io
from typing import Optional

import pytest
from pydantic import ValidationError

from neo4j_app.core import AppConfig
from neo4j_app.tests.conftest import fail_if_exception


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
            """neo4jProject=test-project
neo4jImportDir=import-dir
""",
            AppConfig(
                neo4j_app_host="127.0.0.1",
                neo4j_app_port=8080,
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


@pytest.mark.pull(id="62")
def test_should_support_address_without_port():
    # Given
    config = AppConfig(elasticsearch_address="http://elasticsearch")
    # Then
    with fail_if_exception("Failed to initialize ES client"):
        config.to_es_client()


@pytest.mark.pull(id="91")
def test_should_forward_page_size_to_client():
    # Given
    es_default_page_size = 666
    config = AppConfig(
        elasticsearch_address="http://elasticsearch",
        es_default_page_size=es_default_page_size,
    )
    # When
    client = config.to_es_client()
    # Then
    assert client.pagination_size == es_default_page_size


@pytest.mark.parametrize("user,password", [(None, "somepass"), ("someuser", None)])
def test_should_raise_for_missing_auth_part(
    user: Optional[str], password: Optional[str]
):
    # When/Then
    expected_msg = "neo4j authentication is missing user or password"
    with pytest.raises(ValidationError, match=expected_msg):
        AppConfig(
            elasticsearch_address="http://elasticsearch:9222",
            neo4j_user=user,
            neo4j_password=password,
        )
