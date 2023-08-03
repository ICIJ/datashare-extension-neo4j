import functools
import logging
import traceback
from typing import Dict, Iterable, List

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.utils import is_body_allowed_for_status_code
from starlette import status
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse, Response

from neo4j_app.app.admin import admin_router
from neo4j_app.app.documents import documents_router
from neo4j_app.app.doc import DOCUMENT_TAG, NE_TAG, OTHER_TAG
from neo4j_app.app.graphs import graphs_router
from neo4j_app.app.main import main_router
from neo4j_app.app.named_entities import named_entities_router
from neo4j_app.core import AppConfig
from neo4j_app.core.neo4j import MIGRATIONS, migrate_db_schemas
from neo4j_app.core.neo4j.migrations import delete_all_migrations
from neo4j_app.core.utils.logging import DifferedLoggingMessage

_REQUEST_VALIDATION_ERROR = "Request Validation Error"

_INTERNAL_SERVER_ERROR = "Internal Server Error"

logger = logging.getLogger(__name__)


def json_error(*, title, detail, **kwargs) -> Dict:
    error = {"title": title, "detail": detail}
    error.update(kwargs)
    return error


async def request_validation_error_handler(
    request: Request, exc: RequestValidationError
):
    title = _REQUEST_VALIDATION_ERROR
    detail = exc.errors()
    error = json_error(title=title, detail=detail)
    logger.error(
        "%s\nURL: %s\nDetail: %s",
        title,
        request.url,
        DifferedLoggingMessage(_display_errors, detail),
    )
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content=jsonable_encoder(error)
    )


async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    headers = getattr(exc, "headers", None)
    if not is_body_allowed_for_status_code(exc.status_code):
        return Response(status_code=exc.status_code, headers=headers)
    title = detail = exc.detail
    error = json_error(title=title, detail=detail)
    logger.error("%s\nURL: %s", title, request.url)
    return JSONResponse(
        jsonable_encoder(error), status_code=exc.status_code, headers=headers
    )


async def internal_exception_handler(request: Request, exc: Exception):
    # pylint: disable=unused-argument
    title = _INTERNAL_SERVER_ERROR
    detail = f"{type(exc).__name__}: {exc}"
    trace = "".join(traceback.format_exc())
    error = json_error(title=title, detail=detail, trace=trace)
    logger.error(
        "%s\nURL: %s\nDetail: %s\nTrace: %s",
        title,
        request.url,
        detail,
        trace,
    )
    return JSONResponse(jsonable_encoder(error), status_code=500)


def _make_open_api_tags(tags: Iterable[str]) -> List[Dict]:
    return [{"name": t} for t in tags]


def create_app(config: AppConfig) -> FastAPI:
    app = FastAPI(
        title=config.doc_app_name,
        openapi_tags=_make_open_api_tags([DOCUMENT_TAG, NE_TAG, OTHER_TAG]),
    )
    # Important note: we only put the config in the global state, we provide all
    # persistent DB connection pool, clients and so on through dependency injection.
    # This will allow use to use uvicorn with several workers, each worker correctly
    # handling these objets using FastAPI asyncontextmanagers dependencies. Uvicorn
    #  sadly doesn't support context manager factories
    AppConfig.set_config_globally(config)
    app.state.config = config
    app.add_exception_handler(RequestValidationError, request_validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(Exception, internal_exception_handler)
    app.add_event_handler("startup", app.state.config.setup_loggers)
    app.add_event_handler("startup", functools.partial(migrate_app_dbs, app))
    app.include_router(main_router())
    app.include_router(documents_router())
    app.include_router(named_entities_router())
    app.include_router(admin_router())
    app.include_router(graphs_router())
    return app


async def migrate_app_dbs(app: FastAPI):
    config: AppConfig = app.state.config
    async with config.to_neo4j_driver() as driver:
        logger.info("Running schema migrations at application startup...")
        if config.force_migrations:
            # TODO: improve this as is could lead to race conditions...
            await delete_all_migrations(driver)
        await migrate_db_schemas(
            driver,
            registry=MIGRATIONS,
            timeout_s=config.neo4j_app_migration_timeout_s,
            throttle_s=config.neo4j_app_migration_throttle_s,
        )


def _display_errors(errors: List[Dict]) -> str:
    return "\n".join(
        f'{_display_error_loc(e)}\n  {e["msg"]} ({_display_error_type_and_ctx(e)})'
        for e in errors
    )


def _display_error_loc(error: Dict) -> str:
    return " -> ".join(str(e) for e in error["loc"])


def _display_error_type_and_ctx(error: Dict) -> str:
    t = "type=" + error["type"]
    ctx = error.get("ctx")
    if ctx:
        return t + "".join(f"; {k}={v}" for k, v in ctx.items())
    return t
