import importlib.metadata
from typing import Annotated, Optional

import typer

from icij_common.logging_utils import setup_loggers

import icij_worker
from icij_worker.cli.workers import worker_app

cli_app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
cli_app.add_typer(worker_app)


def version_callback(value: bool):
    if value:
        package_version = importlib.metadata.version(icij_worker.__name__)
        print(package_version)
        raise typer.Exit()


@cli_app.callback(name="icij-worker")
def main(
    version: Annotated[  # pylint: disable=unused-argument
        Optional[bool],
        typer.Option(  # pylint: disable=unused-argument
            "--version", callback=version_callback, is_eager=True
        ),
    ] = None,
):
    """Python async worker pool CLI 🧑‍🏭"""
    setup_loggers(["__main__", icij_worker.__name__])
