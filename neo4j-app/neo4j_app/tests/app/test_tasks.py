# pylint: disable=redefined-outer-name
from functools import partial
from pathlib import Path
from typing import Optional

import neo4j
import pytest
from _pytest.fixtures import FixtureRequest
from icij_common.logging_utils import DifferedLoggingMessage
from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import TEST_PROJECT, true_after
from icij_worker import AsyncApp, Task, TaskStatus
from icij_worker.exceptions import TaskQueueIsFull
from icij_worker.tests.conftest import MockWorkerConfig
from starlette.testclient import TestClient

from neo4j_app.app import tasks
from neo4j_app.app.config import ServiceConfig, WorkerType
from neo4j_app.app.utils import create_app
from neo4j_app.core.objects import TaskJob
from neo4j_app.tests.conftest import MockServiceConfig, test_error_router


@pytest.fixture(scope="function")
def test_client_prod(
    test_config: MockServiceConfig,
    # Wipe neo4j and init project
    neo4j_app_driver: neo4j.AsyncSession,
) -> TestClient:
    # pylint: disable=unused-argument
    prod_deps = "neo4j_app.app.dependencies.HTTP_SERVICE_LIFESPAN_DEPS"
    config_as_dict = test_config.dict(exclude_unset=True)
    update = {
        "neo4j_app_async_app": None,
        "neo4j_app_dependencies": prod_deps,
        "neo4j_app_worker_type": WorkerType.neo4j,
    }
    config_as_dict.update(update)
    config = ServiceConfig(**config_as_dict)
    app = create_app(
        config,
        async_app="icij_worker.utils.tests.APP",
        worker_extras={"teardown_dependencies": False},
    )
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
def test_task_should_return_201_for_created_task(
    task_id: Optional[str], test_client_with_async: TestClient
):
    # Given
    test_client = test_client_with_async
    url = f"/tasks?project={TEST_PROJECT}"
    inputs = {"greeted": "everyone"}
    job = TaskJob(task_id=task_id, task_type="hello_world", inputs=inputs)

    # When
    res = test_client.post(url, json=job.dict())

    # Then
    assert res.status_code == 201, res.json()
    created_task_id = res.text
    assert isinstance(created_task_id, str)
    if task_id is not None:
        assert created_task_id == task_id


@pytest.mark.parametrize("task_id", [None, "some-id"])
def test_task_should_return_200_for_existing_task(
    task_id: Optional[str], test_client_with_async: TestClient
):
    # Given
    test_client = test_client_with_async
    url = f"/tasks?project={TEST_PROJECT}"
    inputs = {"greeted": "everyone"}
    job = TaskJob(task_id=task_id, task_type="hello_world", inputs=inputs)

    # When
    first = test_client.post(url, json=job.dict())
    second = test_client.post(url, json=job.dict())

    # Then
    assert first.status_code == 201, first.json()
    assert second.status_code == 200, second.json()
    assert first.text == second.text


@pytest.mark.parametrize(
    "test_client_type",
    [
        "test_client_with_async",
        "test_client_prod",
    ],
)
def test_task_integration(test_client_type: str, request: FixtureRequest):
    # Given
    test_client = request.getfixturevalue(test_client_type)
    create_url = f"/tasks?project={TEST_PROJECT}"
    inputs = {"greeted": "everyone"}
    job = TaskJob(task_id=None, task_type="hello_world", inputs=inputs)

    # When
    res = test_client.post(create_url, json=job.dict())
    assert res.status_code == 201, res.json()
    task_id = res.content.decode()
    error_url = f"/tasks/{task_id}/errors?project={TEST_PROJECT}"

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
    ), DifferedLoggingMessage(lambda: res.get(error_url).json())
    result_url = f"/tasks/{task_id}/result?project={TEST_PROJECT}"
    res = test_client.get(result_url)
    assert res.status_code == 200, res.json()
    result = res.json()
    assert result == "Hello everyone !"


def test_cancel_task(test_client: TestClient):
    # Given
    inputs = {"greeted": "everyone"}
    job = TaskJob(task_id=None, task_type="hello_world", inputs=inputs)

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


_ASYNC_APP_LIMITED_QUEUE = None


@pytest.fixture(scope="function")
def test_client_limited_queue(
    test_config: MockServiceConfig, test_async_app: AsyncApp, mock_db: Path
) -> TestClient:
    config = safe_copy(
        test_config,
        update={"neo4j_app_task_queue_size": 0, "neo4j_app_async_app": None},
    )
    new_async_app = AsyncApp(
        name=test_async_app.name,
        dependencies=test_async_app._dependencies,  # pylint: disable=protected-access
    )
    new_async_app._registry = (  # pylint: disable=protected-access
        test_async_app.registry
    )
    global _ASYNC_APP_LIMITED_QUEUE
    _ASYNC_APP_LIMITED_QUEUE = new_async_app
    worker_extras = {"teardown_dependencies": False}
    worker_config = MockWorkerConfig(db_path=mock_db)
    app = create_app(
        config,
        worker_config=worker_config,
        async_app=f"{__name__}._ASYNC_APP_LIMITED_QUEUE",
        worker_extras=worker_extras,
    )
    # Add a rout0er which generates error in order to test error handling
    app.include_router(test_error_router())
    with TestClient(app) as client:
        yield client


async def test_create_task_should_return_429_when_too_many_tasks(
    test_client: TestClient, monkeypatch
):
    # Given
    job = TaskJob(task_type="sleep_forever")
    url = f"/tasks?project={TEST_PROJECT}"

    class QueueIsFullTaskManager:
        async def enqueue(self, task: Task, project: str) -> Task:
            raise TaskQueueIsFull(0)

    # When
    res_0 = test_client.post(url, json=job.dict())
    assert res_0.status_code == 201, res_0.json()

    monkeypatch.setattr(tasks, "lifespan_task_manager", QueueIsFullTaskManager)
    res_1 = test_client.post(url, json=job.dict())
    # Then
    assert res_1.status_code == 429, res_1.json()


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
    job = TaskJob(task_id=None, task_type="hello_world", inputs=inputs)

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
