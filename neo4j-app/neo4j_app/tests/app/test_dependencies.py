from unittest import mock
from unittest.mock import MagicMock

import neo4j
from starlette.testclient import TestClient

from neo4j_app.core import AppConfig


def test_should_cache_neo4j_driver(test_client_session: TestClient):
    # Given
    client = test_client_session
    # We have to call a route which depends on the config
    url = "/documents"
    with mock.patch("neo4j_app.app.dependencies.neo4j_driver_dep") as driver_dep_mock:
        driver_dep_mock.return_value = neo4j.AsyncGraphDatabase.driver(
            "neo4j:/localhost:9999"
        )
        # When
        client.post(url, json=dict())
        # Then
        assert driver_dep_mock.call_count == 0
        # When
        client.post(url, json=dict())
        # Then
        assert driver_dep_mock.call_count == 0


def test_should_cache_es_client(test_client_session: TestClient):
    # Given
    client = test_client_session
    # We have to call a route which depends on the config
    url = "/documents"
    with mock.patch("neo4j_app.app.dependencies.es_client_dep") as es_client_dep_mock:
        es_client_dep_mock.return_value = MagicMock()
        # When
        client.post(url, json=dict())
        # Then
        assert es_client_dep_mock.call_count == 0
        # When
        client.post(url, json=dict())
        # Then
        assert es_client_dep_mock.call_count == 0


def test_should_cache_app_config(test_client_session: TestClient):
    # Given
    client = test_client_session
    # We have to call a route which depends on the config
    url = "/documents"
    with mock.patch(
        "neo4j_app.app.dependencies.get_global_config_dep"
    ) as global_config_mock:
        global_config_mock.return_value = AppConfig.get_global_config()
        # When
        client.post(url, json=dict())
        # Then
        assert global_config_mock.call_count == 0
        # When
        client.post(url, json=dict())
        # Then
        assert global_config_mock.call_count == 0
