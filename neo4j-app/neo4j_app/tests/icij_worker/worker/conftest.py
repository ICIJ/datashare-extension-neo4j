from __future__ import annotations

from pathlib import Path

import pytest

from neo4j_app.icij_worker import AsyncApp
from neo4j_app.tests.icij_worker.conftest import MockWorker


@pytest.fixture(scope="module")
def test_app() -> AsyncApp:
    app = AsyncApp(name="test-app", dependencies=[])

    @app.task
    async def hello_word(greeted: str):
        return f"hello {greeted}"

    return app


@pytest.fixture(scope="function")
def mock_worker(test_async_app: AsyncApp, mock_db: Path) -> MockWorker:
    worker = MockWorker(
        test_async_app, "test-worker", mock_db, teardown_dependencies=False
    )
    return worker
