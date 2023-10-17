import importlib.metadata

from fastapi import APIRouter, HTTPException, Response
from starlette.requests import Request

from neo4j_app.app.dependencies import (
    lifespan_es_client,
    lifespan_neo4j_driver,
    lifespan_task_store,
    lifespan_worker_pool,
)
from neo4j_app.app.doc import OTHER_TAG
from neo4j_app.core import AppConfig


def main_router() -> APIRouter:
    router = APIRouter(tags=[OTHER_TAG])

    @router.get("/ping")
    def ping() -> str:
        try:
            lifespan_neo4j_driver()
            lifespan_es_client()
            lifespan_task_store()
            lifespan_worker_pool()
        except Exception as e:  # pylint: disable=broad-except
            raise HTTPException(503, detail="Service Unavailable") from e
        return "pong"

    @router.get("/config", response_model=AppConfig, response_model_exclude_unset=True)
    async def config(request: Request) -> AppConfig:
        if request.app.state.config.supports_neo4j_enterprise is None:
            conf = request.app.state.config
            with_support = await conf.with_neo4j_support()
            request.app.state.config = with_support
        return request.app.state.config

    @router.get("/version")
    def version() -> Response:
        import neo4j_app

        package_version = importlib.metadata.version(neo4j_app.__name__)

        return Response(content=package_version, media_type="text/plain")

    return router
