from starlette.testclient import TestClient

from neo4j_app import ROOT_DIR
from neo4j_app.core import AppConfig

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


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
    config = AppConfig.parse_obj(res.json())
    assert isinstance(config.supports_neo4j_enterprise, bool)


def test_version(test_client: TestClient):
    # Given
    url = "/version"
    pyproject_toml_path = ROOT_DIR.parent.joinpath("pyproject.toml")
    pyproject_toml = tomllib.loads(pyproject_toml_path.read_text())

    # When
    res = test_client.get(url)

    # Then
    assert res.status_code == 200, res.json()
    pyproject_version = pyproject_toml["tool"]["poetry"]["version"]
    assert res.text == pyproject_version
