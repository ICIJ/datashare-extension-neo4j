# pylint: disable=redefined-outer-name
from functools import partial
from typing import Optional

import neo4j
import pytest
from _pytest.fixtures import FixtureRequest
from starlette.testclient import TestClient

from neo4j_app.app.utils import create_app
from neo4j_app.core import AppConfig
from neo4j_app.core.config import WorkerType
from neo4j_app.core.objects import TaskJob
from neo4j_app.core.utils.pydantic import safe_copy
from neo4j_app.icij_worker import ICIJApp, Task, TaskStatus
from neo4j_app.tests.conftest import TEST_PROJECT, test_error_router, true_after


@pytest.fixture(scope="function")
def test_client_prod(
    test_config: AppConfig,
    test_async_app: ICIJApp,
    # Wipe neo4j and init project
    neo4j_app_driver: neo4j.AsyncSession,
) -> TestClient:
    # pylint: disable=unused-argument
    config = safe_copy(
        test_config, update={"neo4j_app_worker_type": WorkerType.NEO4J, "test": False}
    )
    new_async_app = ICIJApp(name=test_async_app.name, config=config)
    new_async_app._registry = (  # pylint: disable=protected-access
        test_async_app.registry
    )
    app = create_app(config, async_app=new_async_app)
    # Add a router which generates error in order to test error handling
    app.include_router(test_error_router())
    with TestClient(app) as client:
        yield client


def _assert_task_has_status(
    client: TestClient, task_id: str, project: str, expected_status: TaskStatus
):
    url = f"/tasks/{task_id}?project={project}"
    res = client.get(url)
    if res.status_code != 200:
        raise ValueError(res.json())
    task = Task.parse_obj(res.json())
    status = task.status
    return status == expected_status


@pytest.mark.parametrize("task_id", [None, "some-id"])
def test_task(task_id: Optional[str], test_client_with_async: TestClient):
    # Given
    test_client = test_client_with_async
    url = f"/tasks?project={TEST_PROJECT}"
    inputs = {"greeted": "everyone"}
    job = TaskJob(task_id=task_id, type="hello_world", inputs=inputs)

    # When
    res = test_client.post(url, json=job.dict())

    # Then
    assert res.status_code == 201, res.json()
    created_task_id = res.content.decode()
    assert isinstance(created_task_id, str)
    if task_id is not None:
        assert created_task_id == task_id


@pytest.mark.parametrize(
    "test_client_type",
    ["test_client_with_async", "test_client_prod"],
)
def test_task_integration(test_client_type: str, request: FixtureRequest):
    # Given
    test_client = request.getfixturevalue(test_client_type)
    create_url = f"/tasks?project={TEST_PROJECT}"
    inputs = {"greeted": "everyone"}
    job = TaskJob(task_id=None, type="hello_world", inputs=inputs)

    # When
    res = test_client.post(create_url, json=job.dict())
    assert res.status_code == 201, res.json()
    task_id = res.content.decode()

    # Then
    assert true_after(
        partial(
            _assert_task_has_status,
            test_client,
            task_id=task_id,
            project=TEST_PROJECT,
            expected_status=TaskStatus.DONE,
        ),
        after_s=2,
    )
    result_url = f"/tasks/{task_id}/result?project={TEST_PROJECT}"
    res = test_client.get(result_url)
    assert res.status_code == 200, res.json()
    result = res.json()
    assert result == "Hello everyone !"


def test_cancel_task(test_client: TestClient):
    # Given
    inputs = {"greeted": "everyone"}
    job = TaskJob(task_id=None, type="hello_world", inputs=inputs)

    # When
    create_url = f"/tasks?project={TEST_PROJECT}"
    res = test_client.post(create_url, json=job.dict())
    assert res.status_code == 201, res.json()
    task_id = res.text

    cancel_url = f"/tasks/{task_id}/cancel?project={TEST_PROJECT}"
    res = test_client.post(cancel_url)
    assert res.status_code == 200, res.json()
    cancelled = Task.parse_obj(res.json())
    assert cancelled.status is TaskStatus.CANCELLED


@pytest.fixture(scope="function")
def test_client_limited_queue(
    test_config: AppConfig, test_async_app: ICIJApp
) -> TestClient:
    config = safe_copy(test_config, update={"neo4j_app_task_queue_size": 0})
    new_async_app = ICIJApp(name=test_async_app.name, config=config)
    new_async_app._registry = (  # pylint: disable=protected-access
        test_async_app.registry
    )
    app = create_app(config, async_app=new_async_app)
    # Add a router which generates error in order to test error handling
    app.include_router(test_error_router())
    with TestClient(app) as client:
        yield client


def test_create_task_should_return_429_when_too_many_tasks(
    test_client_limited_queue: TestClient,
):
    # Given
    test_client = test_client_limited_queue
    url = f"/tasks?project={TEST_PROJECT}"
    job = TaskJob(type="sleep_forever")

    # When
    res_0 = test_client.post(url, json=job.dict())
    res_1 = test_client.post(url, json=job.dict())
    res_2 = test_client.post(url, json=job.dict())

    # Then
    assert res_0.status_code == 201, res_0.json()
    # This one is queued or rejected depending if the first one is processed or still
    # in the queue
    assert res_1.status_code in [201, 429], res_1.json()
    assert res_2.status_code == 429, res_1.json()


def test_get_task_should_return_404_for_unknown_task(
    test_client_with_async: TestClient,
):
    # Given
    test_client = test_client_with_async
    url = f"/tasks/idontexist?project={TEST_PROJECT}"
    # When
    res = test_client.get(url)
    # Then
    assert res.status_code == 404, res.json()
    error = res.json()
    assert error["detail"] == 'Unknown task "idontexist"'


def test_get_task_result_should_return_404_for_unknown_task(
    test_client_with_async: TestClient,
):
    # Given
    test_client = test_client_with_async
    url = f"/tasks/idontexist/result?project={TEST_PROJECT}"
    # When
    res = test_client.get(url)
    # Then
    assert res.status_code == 404, res.json()
    error = res.json()
    assert error["detail"] == 'Unknown task "idontexist"'


def test_get_task_error(test_client_with_async: TestClient):
    # Given
    test_client = test_client_with_async
    create_url = f"/tasks?project={TEST_PROJECT}"
    inputs = {"wrong_arg": None}
    job = TaskJob(task_id=None, type="hello_world", inputs=inputs)

    # When
    res = test_client.post(create_url, json=job.dict())
    assert res.status_code == 201, res.json()
    task_id = res.content.decode()

    # Then
    assert true_after(
        partial(
            _assert_task_has_status,
            test_client,
            task_id=task_id,
            project=TEST_PROJECT,
            expected_status=TaskStatus.ERROR,
        ),
        after_s=2,
    )
    error_url = f"/tasks/{task_id}/errors?project={TEST_PROJECT}"
    res = test_client.get(error_url)
    assert res.status_code == 200, res.json()
    errors = res.json()
    assert len(errors) == 1
    expected = "hello_world() got an unexpected keyword argument"
    assert errors[0]["detail"].startswith(expected)


def test_get_task_error_should_return_404_for_unknown_task(
    test_client_with_async: TestClient,
):
    # Given
    test_client = test_client_with_async
    url = f"/tasks/idontexist/error?project={TEST_PROJECT}"
    # When
    res = test_client.get(url)
    # Then
    assert res.status_code == 404, res.json()
    error = res.json()
    assert error["detail"] == "Not Found"
