# pylint: disable=redefined-outer-name
import os
from typing import ClassVar, Optional

import pytest
from pydantic import Field

from neo4j_app.icij_worker import WorkerConfig


@pytest.fixture()
def env_log_level(reset_env, request):  # pylint: disable=unused-argument
    log_level = request.param
    if log_level is not None:
        os.environ["ICIJ_WORKER_LOG_LEVEL"] = log_level


@WorkerConfig.register()
class WorkerImplConfig(WorkerConfig):
    type: ClassVar[str] = Field(const=True, default="worker_impl")


@pytest.fixture()
def mock_worker_in_env(tmp_path):  # pylint: disable=unused-argument
    os.environ["ICIJ_WORKER_TYPE"] = "worker_impl"
    os.environ["ICIJ_WORKER_DB_PATH"] = str(tmp_path / "mock-db.json")


@pytest.mark.parametrize(
    "env_log_level,expected_level",
    [(None, "INFO"), ("DEBUG", "DEBUG"), ("INFO", "INFO")],
    indirect=["env_log_level"],
)
def test_config_from_env(
    env_log_level: Optional[str], mock_worker_in_env, expected_level: str
):
    # pylint: disable=unused-argument
    # When
    config = WorkerConfig.from_env()
    # Then
    assert isinstance(config, WorkerImplConfig)
    assert config.log_level == expected_level
