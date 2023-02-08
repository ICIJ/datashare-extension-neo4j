from fastapi import APIRouter

_TAGS = ["Other"]


def main_router() -> APIRouter:
    router = APIRouter(tags=_TAGS)

    @router.get("/ping")
    def ping() -> str:
        return "pong"

    return router
