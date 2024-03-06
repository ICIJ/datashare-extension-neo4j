from enum import Enum, unique

from .config import WorkerConfig
from .worker import Worker


@unique
class WorkerType(str, Enum):
    # pylint: disable=invalid-name
    mock = "mock"
    neo4j = "neo4j"
