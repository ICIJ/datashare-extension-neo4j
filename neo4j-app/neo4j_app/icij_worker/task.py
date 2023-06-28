from __future__ import annotations

import traceback
from enum import Enum, unique
from typing import Any, Dict, Optional

from pydantic import Field

from neo4j_app.core.utils.pydantic import (
    IgnoreExtraModel,
    LowerCamelCaseModel,
    NoEnumModel,
)

PROGRESS_HANDLER_ARG = "progress_handler"


@unique
class TaskStatus(Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRY = "RETRY"
    ERROR = "ERROR"
    DONE = "DONE"
    CANCELLED = "CANCELLED"


class Task(NoEnumModel, LowerCamelCaseModel, IgnoreExtraModel):
    id: str
    type: str
    status: TaskStatus
    created_at: str
    inputs: Dict[str, Any] = Field(default_factory=dict)


class TaskEvent(NoEnumModel, LowerCamelCaseModel, IgnoreExtraModel):
    task_id: str
    status: Optional[TaskStatus] = None
    progress: Optional[float] = None
    error: Optional[str] = None
    retries: Optional[int] = None


class TaskResult(LowerCamelCaseModel, IgnoreExtraModel):
    task_id: str
    result: object


class TaskError(LowerCamelCaseModel, IgnoreExtraModel):
    # Follow the "problem detail" spec:
    # https://datatracker.ietf.org/doc/html/draft-ietf-appsawg-http-problem-00
    task_id: str
    title: str
    detail: str

    @classmethod
    def from_exception(cls, exception: Exception, task_id: str) -> TaskError:
        title = exception.__class__.__name__
        trace_lines = traceback.format_exception(
            None, value=exception, tb=exception.__traceback__
        )
        detail = f"{exception}\n{''.join(trace_lines)}"
        return TaskError(task_id=task_id, title=title, detail=detail)
