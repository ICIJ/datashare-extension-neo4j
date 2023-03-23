from fastapi import APIRouter, Depends, Response

from neo4j_app.app.dependencies import get_global_config_dep
from neo4j_app.core import AppConfig

OTHER_TAG = "Other"


def main_router() -> APIRouter:
    router = APIRouter(tags=[OTHER_TAG])

    @router.get("/ping")
    def ping() -> str:
        return "pong"

    @router.get("/config", response_model=AppConfig, response_model_exclude_unset=True)
    def config(config: AppConfig = Depends(get_global_config_dep)) -> AppConfig:
        return config

    @router.get("/version")
    def version() -> Response:
        import neo4j_app

        return Response(content=neo4j_app.__version__, media_type="text/plain")

    return router
