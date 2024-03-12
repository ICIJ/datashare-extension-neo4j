# pylint: disable=redefined-outer-name
# Test utils meant to be imported from clients libs to test their implem of workers
import asyncio
import json
import logging
import tempfile
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from icij_common.pydantic_utils import ICIJModel, jsonable_encoder

import icij_worker
from icij_worker import AsyncApp
from icij_worker.typing_ import PercentProgress
from icij_worker.utils.dependencies import DependencyInjectionError
from icij_worker.utils.logging_ import LogWithWorkerIDMixin

logger = logging.getLogger(__name__)

_has_pytest = False  # necessary because of the pytest decorators which requires pytest
# to be defined
try:
    import pytest

    _has_pytest = True
except ImportError:
    pass

if _has_pytest:

    class DBMixin(ABC):
        _task_collection = "tasks"
        _error_collection = "errors"
        _result_collection = "results"

        def __init__(self, db_path: Path):
            self._db_path = db_path

        @property
        def db_path(self) -> Path:
            return self._db_path

        def _write(self, data: Dict):
            self._db_path.write_text(json.dumps(jsonable_encoder(data)))

        def _read(self):
            return json.loads(self._db_path.read_text())

        @staticmethod
        def _task_key(task_id: str, project: str) -> str:
            return str((task_id, project))

        @classmethod
        def fresh_db(cls, db_path: Path):
            db = {
                cls._task_collection: dict(),
                cls._error_collection: {},
                cls._result_collection: {},
            }
            db_path.write_text(json.dumps(db))

    @pytest.fixture(scope="session")
    def mock_db_session() -> Path:
        with tempfile.NamedTemporaryFile(prefix="mock-db", suffix=".json") as f:
            db_path = Path(f.name)
            DBMixin.fresh_db(db_path)
            yield db_path

    @pytest.fixture
    def mock_db(mock_db_session: Path) -> Path:
        # Wipe the DB
        DBMixin.fresh_db(mock_db_session)
        return mock_db_session

    class MockAppConfig(ICIJModel, LogWithWorkerIDMixin):
        # Just provide logging stuff to be able to see nice logs while doing TDD
        log_level: str = "DEBUG"
        loggers: List[str] = [icij_worker.__name__]

    _MOCKED_CONFIG: Optional[MockAppConfig] = None

    async def mock_async_config_enter(**_):
        global _MOCKED_CONFIG
        _MOCKED_CONFIG = MockAppConfig()
        logger.info("Loading mocked configuration %s", _MOCKED_CONFIG.json(indent=2))

    def lifespan_config() -> MockAppConfig:
        if _MOCKED_CONFIG is None:
            raise DependencyInjectionError("config")
        return _MOCKED_CONFIG

    def loggers_enter(worker_id: str, **_):
        config = lifespan_config()
        config.setup_loggers(worker_id=worker_id)
        logger.info("worker loggers ready to log 💬")

    mocked_app_deps = [
        ("configuration loading", mock_async_config_enter, None),
        ("loggers setup", loggers_enter, None),
    ]

    APP = AsyncApp(name="test-app", dependencies=mocked_app_deps)

    @APP.task
    async def hello_world(
        greeted: str, progress: Optional[PercentProgress] = None
    ) -> str:
        if progress is not None:
            await progress(0.1)
        greeting = f"Hello {greeted} !"
        if progress is not None:
            await progress(0.99)
        return greeting

    @APP.task
    def hello_world_sync(greeted: str) -> str:
        greeting = f"Hello {greeted} !"
        return greeting

    @APP.task
    async def sleep_for(
        duration: float, s: float = 0.01, progress: Optional[PercentProgress] = None
    ):
        start = datetime.now()
        elapsed = 0
        while elapsed < duration:
            elapsed = (datetime.now() - start).total_seconds()
            await asyncio.sleep(s)
            if progress is not None:
                await progress(elapsed / duration * 100)

    @pytest.fixture(scope="session")
    def test_async_app() -> AsyncApp:
        return AsyncApp.load(f"{__name__}.APP")
