from fastapi import APIRouter, Depends

from neo4j_app.app.dependencies import get_global_config_dep
from neo4j_app.core import AppConfig

OTHER_TAG = "Other"


def main_router() -> APIRouter:
    router = APIRouter(tags=[OTHER_TAG])

    @router.get("/ping")
    def ping() -> str:
        return "pong"

    @router.get("/config")
    def config(config: AppConfig = Depends(get_global_config_dep)) -> AppConfig:
        return config

    return router
