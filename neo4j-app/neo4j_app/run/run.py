# TODO: rename this into run_http ?
import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import uvicorn

from neo4j_app.app.utils import create_app
from neo4j_app.core.config import AppConfig

DATA_DIR = Path(__file__).parents[3].joinpath(".data")
NEO4J_TEST_IMPORT_DIR = DATA_DIR.joinpath("neo4j", "import")
NEO4J_IMPORT_PREFIX = Path(os.sep).joinpath(".neo4j", "import")


def debug_app():
    neo4j_import_dir = Path(__file__).parents[4].joinpath(".data", "neo4j", "import")
    config = AppConfig(
        neo4j_import_dir=str(neo4j_import_dir),
        neo4j_project="Debug project",
    )
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
        config = AppConfig(
            neo4j_project="test-datashare-project",
            neo4j_import_dir=str(NEO4J_TEST_IMPORT_DIR),
            neo4j_import_prefix=str(NEO4J_IMPORT_PREFIX),
        )
    app = create_app(config)
    uvicorn_config = config.to_uvicorn()
    uvicorn.run(app, **uvicorn_config.dict())


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


def main():
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()

    if hasattr(args, "func"):
        args.func(args)
    else:
        arg_parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
