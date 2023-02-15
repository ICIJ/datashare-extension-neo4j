import pytest
from starlette.testclient import TestClient

from neo4j_app.core import AppConfig


def test_ping(test_client: TestClient):
    # Given
    url = "/ping"

    # When
    res = test_client.get(url)

    # Then
    assert res.status_code == 200, res.json()


def test_config(test_client: TestClient):
    # Given
    url = "/config"

    # When
    res = test_client.get(url)

    # Then
    assert res.status_code == 200, res.json()
    try:
        AppConfig.parse_obj(res.json())
    except:  # pylint: disable=bare-except
        pytest.fail(f"Failed to parse response as a {AppConfig.__name__}")
