import importlib.metadata

from fastapi import APIRouter, HTTPException, Response
from neo4j.exceptions import DriverError
from starlette.requests import Request

from neo4j_app.app import ServiceConfig
from neo4j_app.app.dependencies import (
    DependencyInjectionError,
    lifespan_task_manager,
    lifespan_worker_pool_is_running,
)
from neo4j_app.app.doc import OTHER_TAG
from neo4j_app.tasks.dependencies import lifespan_es_client, lifespan_neo4j_driver


def main_router() -> APIRouter:
    router = APIRouter(tags=[OTHER_TAG])

    @router.get("/ping")
    async def ping() -> str:
        try:
            driver = lifespan_neo4j_driver()
            await driver.verify_connectivity()
            lifespan_es_client()
            lifespan_task_manager()
            lifespan_worker_pool_is_running()
        except (DriverError, DependencyInjectionError) as e:
            raise HTTPException(503, detail="Service Unavailable") from e
        return "pong"

    @router.get(
        "/config", response_model=ServiceConfig, response_model_exclude_unset=True
    )
    async def config(request: Request) -> ServiceConfig:
        if (
            request.app.state.config.supports_neo4j_enterprise is None
            or request.app.state.config.supports_neo4j_parallel_runtime is None
        ):
            msg = (
                "neo4j support has not been set, config has not been properly"
                " initialized using AppConfig.with_neo4j_support"
            )
            raise ValueError(msg)
        return request.app.state.config

    @router.get("/version")
    def version() -> Response:
        import neo4j_app

        package_version = importlib.metadata.version(neo4j_app.__name__)

        return Response(content=package_version, media_type="text/plain")

    return router
