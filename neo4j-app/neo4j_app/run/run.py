# TODO: rename this into run_http ?
import sys
from pathlib import Path

import uvicorn

from neo4j_app.core.config import AppConfig
from neo4j_app.app.utils import create_app


def debug_app():
    neo4j_import_dir = Path(__file__).parents[4].joinpath(".data", "neo4j", "import")
    config = AppConfig(
        neo4j_import_dir=str(neo4j_import_dir), neo4j_project="Debug project"
    )
    app = create_app(config)
    return app


def main():
    # TODO: add an argument parser if need
    if len(sys.argv) > 1:
        config_path = Path(sys.argv[1])
        if not config_path.exists():
            raise ValueError(f"Provided config path does not exists: {config_path}")
        with config_path.open() as f:
            config = AppConfig.from_java_properties(f)
    else:
        raise ValueError("Config path is missing")
    app = create_app(config)
    uvicorn_config = config.to_uvicorn()

    uvicorn.run(app, **uvicorn_config.dict())


if __name__ == "__main__":
    main()
