from typing import Optional

from fastapi import FastAPI

from neo4j_app.app.main import main_router
from neo4j_app.core import AppConfig


def create_app(config: Optional[AppConfig] = None) -> FastAPI:
    if config is None:
        config = AppConfig()
    app = FastAPI(title=config.neo4j_app_name)
    app.include_router(main_router())
    return app
