import pytest
from starlette.testclient import TestClient

from neo4j_app.run.utils import create_app


@pytest.fixture(scope="session")
def test_client() -> TestClient:
    app = create_app()
    with TestClient(app) as client:
        yield client
