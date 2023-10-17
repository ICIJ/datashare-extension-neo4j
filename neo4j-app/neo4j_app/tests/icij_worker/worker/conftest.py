import pytest

from neo4j_app.core import AppConfig
from neo4j_app.icij_worker import ICIJApp


@pytest.fixture(scope="module")
def test_app(test_config: AppConfig) -> ICIJApp:
    app = ICIJApp(name="test-app", config=test_config)

    @app.task
    async def hello_word(greeted: str):
        return f"hello {greeted}"

    return app
