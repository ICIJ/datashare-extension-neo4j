from fastapi import APIRouter

OTHER_TAG = "Other"


def main_router() -> APIRouter:
    router = APIRouter(tags=[OTHER_TAG])

    @router.get("/ping")
    def ping() -> str:
        return "pong"

    return router
