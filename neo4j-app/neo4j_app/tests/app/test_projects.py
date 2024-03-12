import neo4j
from icij_common.neo4j.migrate import init_project
from starlette.testclient import TestClient

from neo4j_app.core.neo4j import V_0_1_0

_BASE_REGISTRY = [V_0_1_0]


def test_project_init_should_return_201(test_client: TestClient):
    # Given
    project_name = "test-project"

    # When
    res = test_client.post(f"/projects/init?project={project_name}")

    # Then
    assert res.status_code == 201


async def test_project_init_should_return_200(
    test_client: TestClient, neo4j_test_driver: neo4j.AsyncDriver
):
    # Given
    project_name = "test-project"
    await init_project(
        neo4j_test_driver, project_name, _BASE_REGISTRY, timeout_s=30, throttle_s=0.1
    )

    # When
    res = test_client.post(f"/projects/init?project={project_name}")

    # Then
    assert res.status_code == 200
