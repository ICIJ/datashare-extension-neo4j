import io
from typing import Optional

import pytest
from pydantic import ValidationError

from neo4j_app.app import ServiceConfig
from neo4j_app.tests.conftest import fail_if_exception


def test_should_support_alias():
    # When
    neo4j_app_name = "test_name"
    config = ServiceConfig(neo4j_app_name=neo4j_app_name)

    # Then
    assert config.neo4j_app_name == neo4j_app_name


@pytest.mark.parametrize(
    "config_as_str,expected_config,expected_written_config",
    [
        ("someExtraInfo=useless", ServiceConfig(), ""),
        (
            """elasticsearchAddress=http://elasticsearch:9222
neo4jAppHost=this-the-neo4j-app
neo4jAppPort=3333""",
            ServiceConfig(
                neo4j_app_host="this-the-neo4j-app",
                neo4j_app_port=3333,
                elasticsearch_address="http://elasticsearch:9222",
            ),
            """elasticsearchAddress=http://elasticsearch:9222
neo4jAppHost=this-the-neo4j-app
neo4jAppPort=3333

""",
        ),
    ],
)
def test_should_load_from_java_and_write_to_java(
    config_as_str: str, expected_config: ServiceConfig, expected_written_config: str
):
    # Given
    config_stream = io.StringIO(config_as_str)

    # When
    loaded_config = ServiceConfig.from_java_properties(config_stream)
    config_io = io.StringIO()
    loaded_config.write_java_properties(config_io)
    written = config_io.getvalue()

    # Then
    assert loaded_config == expected_config
    assert written == expected_written_config


@pytest.mark.pull(id="62")
def test_should_support_address_without_port():
    # Given
    config = ServiceConfig(elasticsearch_address="http://elasticsearch")
    # Then
    with fail_if_exception("Failed to initialize ES client"):
        config.to_es_client()


@pytest.mark.pull(id="91")
def test_should_forward_page_size_to_client():
    # Given
    es_default_page_size = 666
    config = ServiceConfig(
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
        ServiceConfig(
            elasticsearch_address="http://elasticsearch:9222",
            neo4j_user=user,
            neo4j_password=password,
        )
