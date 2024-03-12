import pytest
from typer.testing import CliRunner


@pytest.fixture(scope="session")
def cli_runner() -> CliRunner:
    return CliRunner(mix_stderr=False)
