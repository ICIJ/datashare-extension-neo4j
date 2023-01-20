from fastapi import APIRouter


def main_router() -> APIRouter:
    router = APIRouter()

    @router.get("/ping")
    def ping() -> str:
        return "pong"

    return router
