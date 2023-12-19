from __future__ import annotations

import threading
from pathlib import Path

import pytest

from neo4j_app.core import AppConfig
from neo4j_app.icij_worker import ICIJApp
from neo4j_app.tests.icij_worker.conftest import MockWorker


@pytest.fixture(scope="module")
def test_app(test_config: AppConfig) -> ICIJApp:
    app = ICIJApp(name="test-app", config=test_config)

    @app.task
    async def hello_word(greeted: str):
        return f"hello {greeted}"

    return app


@pytest.fixture(scope="function")
def mock_worker(test_async_app: ICIJApp, tmpdir: Path) -> MockWorker:
    db_path = Path(tmpdir) / "db.json"
    MockWorker.fresh_db(db_path)
    lock = threading.Lock()
    worker = MockWorker(test_async_app, "test-worker", db_path, lock)
    return worker
