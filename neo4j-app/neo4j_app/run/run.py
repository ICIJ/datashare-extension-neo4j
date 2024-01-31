from __future__ import annotations
import argparse
import logging
import multiprocessing
import sys
import traceback
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from gunicorn.app.base import BaseApplication

import neo4j_app
from neo4j_app.app import ServiceConfig
from neo4j_app.app.utils import create_app
from neo4j_app.core.utils.logging import DATE_FMT, STREAM_HANDLER_FMT


class Formatter(argparse.ArgumentDefaultsHelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=35, width=150)


def _start_app_(ns):
    _start_app(config_path=ns.config_path, force_migrations=ns.force_migrations)


class GunicornApp(BaseApplication):  # pylint: disable=abstract-method
    def __init__(self, app: FastAPI, config: ServiceConfig, **kwargs):
        self.application = app
        self._app_config = config
        super().__init__(**kwargs)

    def load_config(self):
        self.cfg.set("worker_class", "uvicorn.workers.UvicornWorker")
        self.cfg.set("workers", self._app_config.neo4j_app_gunicorn_workers)
        bind = f"{self._app_config.neo4j_app_host}:{self._app_config.neo4j_app_port}"
        self.cfg.set("bind", bind)

    def load(self):
        return self.application

    @classmethod
    def from_config(cls, config: ServiceConfig) -> GunicornApp:
        fast_api = create_app(config)
        return cls(fast_api, config)


def _start_app(config_path: Optional[str] = None, force_migrations: bool = False):
    if config_path is not None:
        config_path = Path(config_path)
        if not config_path.exists():
            raise ValueError(f"Provided config path does not exists: {config_path}")
        with config_path.open() as f:
            config = ServiceConfig.from_java_properties(
                f, force_migrations=force_migrations
            )
    else:
        config = ServiceConfig()
    app = GunicornApp.from_config(config)
    app.run()


def get_arg_parser():
    arg_parser = argparse.ArgumentParser(
        description="neo4j_app start CLI", formatter_class=Formatter
    )
    arg_parser.add_argument(
        "--config-path",
        type=str,
        help="Path to Java properties holding the app configuration",
    )
    arg_parser.add_argument(
        "--force-migrations", action="store_true", help="Force migrations to re-run"
    )
    arg_parser.set_defaults(func=_start_app_)
    return arg_parser


def _setup_loggers():
    loggers = [neo4j_app.__name__, "__main__"]
    level = logging.INFO
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(logging.Formatter(STREAM_HANDLER_FMT, DATE_FMT))
    for logger in loggers:
        logger = logging.getLogger(logger)
        logger.setLevel(level)
        logger.handlers = []
        logger.addHandler(stream_handler)


def main():
    # Setup loggers temporarily before loggers init using the app configuration
    _setup_loggers()
    logger = logging.getLogger(__name__)
    try:
        arg_parser = get_arg_parser()
        args = arg_parser.parse_args()

        if hasattr(args, "func"):
            args.func(args)
        else:
            arg_parser.print_help()
            sys.exit(1)
    except KeyboardInterrupt as e:
        logger.error("Application shutdown...")
        raise e
    except Exception as e:  # pylint: disable=broad-except:
        error_with_trace = "".join(traceback.format_exception(None, e, e.__traceback__))
        logger.error("Error occurred at application startup:\n%s", error_with_trace)
        raise e


if __name__ == "__main__":
    # https://github.com/pyinstaller/pyinstaller/wiki/Recipe-Multiprocessing
    multiprocessing.freeze_support()

    main()
