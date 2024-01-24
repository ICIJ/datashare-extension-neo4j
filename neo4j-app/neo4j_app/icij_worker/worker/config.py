from abc import ABC

from pydantic import Field

from neo4j_app.icij_worker.utils.registrable import RegistrableConfig


class WorkerConfig(RegistrableConfig, ABC):
    registry_key: str = Field(const=True, default="type")
    log_level: str = "INFO"
    type: str

    class Config:
        env_prefix = "ICIJ_WORKER_"
        case_sensitive = False
