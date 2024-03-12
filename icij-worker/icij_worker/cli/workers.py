from pathlib import Path
from typing import Annotated, Optional

import typer

from icij_worker import AsyncApp, WorkerBackend, WorkerConfig
from icij_worker.backend import start_workers

_START_HELP = "Start a pool of workers running the provided app, reading the worker\
 configuration from the environment or an optionally provided file."
_APP_HELP = f'Path of the {AsyncApp.__name__} instance to run, fully qualified\
 ("module.sub.module.app_instance")'
_CONFIG_HELP = f"""Path to a serialized {WorkerConfig.__name__} JSON file.
By default, the configuration is read from the environment.
If present, file values will override environment variables values."""
_N_HELP = "Number of workers."
_BACKEND_HELP = "Python asynchronous backend used to create the worker pool."

_DEFAULT_BACKEND = WorkerBackend.MULTIPROCESSING

worker_app = typer.Typer(name="workers")


@worker_app.command(help=_START_HELP)
def start(
    app: Annotated[str, typer.Argument(help=_APP_HELP)],
    config: Annotated[
        Optional[Path], typer.Option("-c", "--config", help=_CONFIG_HELP)
    ] = None,
    n: Annotated[int, typer.Option("--n-workers", "-n", help=_N_HELP)] = 1,
    backend: Annotated[
        WorkerBackend,
        typer.Option(
            help=_BACKEND_HELP,
            case_sensitive=False,
            show_default=_DEFAULT_BACKEND.value,
        ),
    ] = _DEFAULT_BACKEND,
):
    start_workers(app=app, n_workers=n, config_path=config, backend=backend)
