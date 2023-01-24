import sys
from pathlib import Path

import uvicorn

from neo4j_app.core.config import AppConfig
from neo4j_app.run.utils import create_app


def main():
    # TODO: add an argument parser if need
    if len(sys.argv) > 1:
        config_path = Path(sys.argv[1])
        with config_path.open() as f:
            config = AppConfig.from_java_properties(f)
    else:
        config = AppConfig()

    app = create_app(config)
    uvicorn_config = config.to_uvicorn()

    uvicorn.run(app, **uvicorn_config.dict())


if __name__ == "__main__":
    main()
