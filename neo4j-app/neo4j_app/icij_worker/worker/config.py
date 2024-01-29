from pydantic import Field

from neo4j_app.icij_worker.utils.registrable import RegistrableConfig


class WorkerConfig(RegistrableConfig):
    registry_key: str = Field(const=True, default="type")
    cancelled_tasks_refresh_interval_s: int = 2
    task_queue_poll_interval_s: int = 1
    log_level: str = "INFO"
    type: str

    class Config:
        env_prefix = "ICIJ_WORKER_"
