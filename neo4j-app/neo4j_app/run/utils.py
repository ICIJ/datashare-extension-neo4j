from fastapi import FastAPI

from neo4j_app.app.main import main_router


def create_app() -> FastAPI:
    app = FastAPI(title="neo4j app")
    app.include_router(main_router())
    return app
