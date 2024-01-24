# pylint: disable=redefined-outer-name
import os
from typing import Optional

import pytest

from neo4j_app.icij_worker import WorkerConfig


@pytest.fixture()
def env_log_level(reset_env, request):  # pylint: disable=unused-argument
    log_level = request.param
    if log_level is not None:
        os.environ["ICIJ_WORKER_LOG_LEVEL"] = log_level


@pytest.mark.parametrize(
    "env_log_level,expected_level",
    [(None, "INFO"), ("DEBUG", "DEBUG"), ("INFO", "INFO")],
    indirect=["env_log_level"],
)
def test_config_from_env(
    env_log_level: Optional[str], expected_level: str  # pylint: disable=unused-argument
):
    # When
    class WorkerImplConfig(WorkerConfig):
        type: str = "worker_impl"

    config = WorkerImplConfig()
    # Then
    assert config.log_level == expected_level
