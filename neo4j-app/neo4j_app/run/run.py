# TODO: rename this into run_http ?
import argparse
import logging
import sys
import traceback
from pathlib import Path
from typing import Optional

import uvicorn

import neo4j_app
from neo4j_app.app.utils import create_app
from neo4j_app.core.config import AppConfig
from neo4j_app.core.utils.logging import DATE_FMT, STREAM_HANDLER_FMT


def debug_app():
    config = AppConfig()
    app = create_app(config)
    return app


class Formatter(argparse.ArgumentDefaultsHelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=35, width=150)


def _start_app_(ns):
    _start_app(config_path=ns.config_path, force_migrations=ns.force_migrations)


def _start_app(config_path: Optional[str] = None, force_migrations: bool = False):
    if config_path is not None:
        config_path = Path(config_path)
        if not config_path.exists():
            raise ValueError(f"Provided config path does not exists: {config_path}")
        with config_path.open() as f:
            config = AppConfig.from_java_properties(
                f, force_migrations=force_migrations
            )
    else:
        config = AppConfig()
    app = create_app(config)
    uvicorn_config = config.to_uvicorn()
    uvicorn.run(app, **uvicorn_config.dict(by_alias=False))


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
    main()
