import importlib.metadata

from fastapi import APIRouter, Response
from starlette.requests import Request

from neo4j_app.app.doc import OTHER_TAG
from neo4j_app.core import AppConfig


def main_router() -> APIRouter:
    router = APIRouter(tags=[OTHER_TAG])

    @router.get("/ping")
    def ping() -> str:
        return "pong"

    @router.get("/config", response_model=AppConfig, response_model_exclude_unset=True)
    async def config(request: Request) -> AppConfig:
        if request.app.state.config is None:
            request.app.state.config = await config.with_neo4j_support()
        return request.app.state.config

    @router.get("/version")
    def version() -> Response:
        import neo4j_app

        package_version = importlib.metadata.version(neo4j_app.__name__)

        return Response(content=package_version, media_type="text/plain")

    return router
