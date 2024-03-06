from __future__ import annotations

import json
import logging
import traceback
import uuid
from datetime import datetime
from enum import Enum, unique
from typing import Any, Dict, Optional

import neo4j
from pydantic import validator

from neo4j_app.constants import TASK_NODE
from neo4j_app.core.utils.pydantic import (
    ISODatetime,
    LowerCamelCaseModel,
    NoEnumModel,
    safe_copy,
)

logger = logging.getLogger(__name__)

PROGRESS_HANDLER_ARG = "progress_handler"
_TASK_SCHEMA = None


@unique
class TaskStatus(Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"
    CANCELLED = "CANCELLED"

    @classmethod
    def resolve_event_status(cls, stored: Task, event: TaskEvent) -> TaskStatus:
        # A done task is always done
        if stored.status is TaskStatus.DONE:
            return stored.status
        # A task store as ready can't be updated unless there's a new ready state
        # (for instance ERROR -> DONE)
        if stored.status in READY_STATES and event.status not in READY_STATES:
            return stored.status
        if event.status is TaskStatus.QUEUED and stored.status is TaskStatus.RUNNING:
            # We have to store the most recent status
            if event.retries is None:
                return stored.status
            if stored.retries is None or event.retries > stored.retries:
                return event.status
            return stored.status
        # Otherwise the true status is the most advanced on in the state machine
        return max(stored.status, event.status)

    def __gt__(self, other: TaskStatus) -> bool:
        return status_precedence(self) < status_precedence(other)

    def __ge__(self, other: TaskStatus) -> bool:
        return status_precedence(self) <= status_precedence(other)

    def __lt__(self, other: TaskStatus) -> bool:
        return status_precedence(self) > status_precedence(other)

    def __le__(self, other: TaskStatus) -> bool:
        return status_precedence(self) >= status_precedence(other)


READY_STATES = frozenset({TaskStatus.DONE, TaskStatus.ERROR, TaskStatus.CANCELLED})
# Greatly inspired from Celery
PRECEDENCE = [
    TaskStatus.DONE,
    TaskStatus.ERROR,
    TaskStatus.CANCELLED,
    TaskStatus.RUNNING,
    TaskStatus.QUEUED,
    TaskStatus.CREATED,
]
PRECEDENCE_LOOKUP = dict(zip(PRECEDENCE, range(len(PRECEDENCE))))


def status_precedence(state: TaskStatus) -> int:
    return PRECEDENCE_LOOKUP[state]


class Task(NoEnumModel, LowerCamelCaseModel, ISODatetime):
    id: str
    type: str
    inputs: Optional[Dict[str, Any]] = None
    status: TaskStatus
    progress: Optional[float] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    retries: Optional[int] = None

    @validator("inputs", pre=True, always=True)
    def inputs_as_dict(cls, v: Optional[Dict[str, Any]]):
        # pylint: disable=no-self-argument
        if v is None:
            v = dict()
        return v

    @classmethod
    def create(
        cls, *, task_id: str, task_ype: str, task_inputs: Dict[str, Any]
    ) -> Task:
        created_at = datetime.now()
        status = TaskStatus.CREATED
        return cls(
            id=task_id,
            type=task_ype,
            inputs=task_inputs,
            created_at=created_at,
            status=status,
        )

    @validator("inputs", pre=True)
    def _validate_inputs(cls, value: Any):  # pylint: disable=no-self-argument
        if isinstance(value, str):
            value = json.loads(value)
        return value

    @validator("created_at", pre=True)
    def _validate_created_at(cls, value: Any):  # pylint: disable=no-self-argument
        if isinstance(value, neo4j.time.DateTime):
            value = value.to_native()
        return value

    @validator("progress")
    def _validate_progress(cls, value: Optional[float]):
        # pylint: disable=no-self-argument
        if isinstance(value, float) and not 0 <= value <= 100:
            # We log here rather than raising since otherwise a single invalid log will
            # prevent anything any deserialization related
            logger.error("progress is expected to be in [0, 100], found %s", value)
        return value

    @classmethod
    def from_neo4j(cls, record: neo4j.Record, key="task") -> Task:
        node = record[key]
        labels = node.labels
        node = dict(node)
        if len(labels) != 2:
            raise ValueError(f"Expected task to have exactly 2 labels found {labels}")
        status = [label for label in labels if label != TASK_NODE]
        if len(status) != 1:
            raise ValueError(f"Invalid task labels {labels}")
        status = status[0]
        if "completedAt" in node:
            node["completedAt"] = node["completedAt"].to_native()
        if "inputs" in node:
            node["inputs"] = json.loads(node["inputs"])
        return cls(status=status, **node)

    @classmethod
    def mandatory_fields(cls, event: TaskEvent | Task, keep_id: bool) -> Dict[str, Any]:
        event = event.dict(by_alias=True, exclude_unset=True)
        mandatory = dict()
        for f, v in event.items():
            task_field = f.replace("task", "")
            task_field = f"{task_field[0].lower()}{task_field[1:]}"
            if task_field == "id" and not keep_id:
                continue
            if task_field not in cls._schema(by_alias=True)["required"]:
                continue
            mandatory[task_field] = v
        return mandatory

    def resolve_event(self, event: TaskEvent) -> Optional[TaskEvent]:
        if self.status in READY_STATES:
            return None
        resolved = event.dict(exclude_unset=True, by_alias=False)
        resolved.pop("task_id")
        resolved.pop("created_at", None)
        resolved.pop("task_type", None)
        resolved.pop("completed_at", None)
        # Update the status to make it consistent in case of race condition
        if event.status is not None:
            resolved["status"] = TaskStatus.resolve_event_status(self, event)
        # Copy the event a first time to unset non-updatable field
        if not resolved:
            return None
        base_resolved = TaskEvent(task_id=event.task_id)
        resolved = safe_copy(base_resolved, update=resolved)
        return resolved

    @classmethod
    def _schema(cls, by_alias: bool) -> Dict[str, Any]:
        global _TASK_SCHEMA
        if _TASK_SCHEMA is None:
            _TASK_SCHEMA = dict()
            _TASK_SCHEMA[True] = cls.schema(by_alias=True)
            _TASK_SCHEMA[False] = cls.schema(by_alias=False)
        return _TASK_SCHEMA[by_alias]


class TaskError(LowerCamelCaseModel):
    # This helps to know if an error has already been processed or not
    id: str
    # Follow the "problem detail" spec: https://datatracker.ietf.org/doc/html/rfc9457,
    # the type is omitted for now since we gave no URI to resolve errors yet
    title: str
    detail: str
    occurred_at: datetime

    @classmethod
    def from_exception(cls, exception: BaseException) -> TaskError:
        title = exception.__class__.__name__
        trace_lines = traceback.format_exception(
            None, value=exception, tb=exception.__traceback__
        )
        detail = f"{exception}\n{''.join(trace_lines)}"
        error_id = f"{_id_title(title)}-{uuid.uuid4().hex}"
        error = TaskError(
            id=error_id, title=title, detail=detail, occurred_at=datetime.now()
        )
        return error

    @classmethod
    def from_neo4j(cls, record: neo4j.Record, key="error") -> TaskError:
        task = dict(record.value(key))
        if "occurredAt" in task:
            task["occurredAt"] = task["occurredAt"].to_native()
        return cls(**task)


class TaskEvent(NoEnumModel, LowerCamelCaseModel):
    task_id: str
    task_type: Optional[str] = None
    status: Optional[TaskStatus] = None
    progress: Optional[float] = None
    retries: Optional[int] = None
    error: Optional[TaskError] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @classmethod
    def from_task(cls, task: Task):
        as_event = task.dict()
        as_event["task_id"] = as_event.pop("id")
        as_event["task_type"] = as_event.pop("type")
        as_event.pop("inputs")
        return cls(**as_event)

    @classmethod
    def from_error(
        cls, error: TaskError, task_id: str, retries: Optional[int] = None
    ) -> TaskEvent:
        status = TaskStatus.QUEUED if retries is not None else TaskStatus.ERROR
        event = TaskEvent(task_id=task_id, status=status, retries=retries, error=error)
        return event


class TaskResult(LowerCamelCaseModel):
    task_id: str
    result: object

    @classmethod
    def from_neo4j(
        cls, record: neo4j.Record, task_key="task", result_key="result"
    ) -> TaskResult:
        result = record.get(result_key)
        if result is not None:
            result = json.loads(result["result"])
        return cls(task_id=record[task_key]["id"], result=result)


def _id_title(title: str) -> str:
    id_title = []
    for i, letter in enumerate(title):
        if i and letter.isupper():
            id_title.append("-")
        id_title.append(letter.lower())
    return "".join(id_title)
