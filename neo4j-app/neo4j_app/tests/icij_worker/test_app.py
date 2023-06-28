from typing import Optional

import pytest

from neo4j_app.icij_worker import ICIJApp


@pytest.mark.parametrize(
    "task_name,expected_name",
    [(None, "hello_world"), ("hello", "hello")],
)
def test_should_register_task_with_name(task_name: Optional[str], expected_name: str):
    # Given
    app = ICIJApp(name="test-app")

    # When
    if task_name is None:

        @app.task
        def hello_world():
            pass

    else:

        @app.task(name=task_name)
        def hello_world():
            pass

    # Then
    assert expected_name in app.registry


def test_should_raise_for_already_registered_name():
    # Given
    app = ICIJApp(name="test-app")

    # When
    @app.task
    def hello_world():
        pass

    # Then
    with pytest.raises(ValueError, match='A task "hello_world" was already registered'):

        @app.task(name="hello_world")
        def another_hello_world():
            pass
