from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum, unique
from typing import Any, Dict, List, Optional

from pydantic import Field

from neo4j_app.core.utils.pydantic import LowerCamelCaseModel, NoEnumModel
from neo4j_app.icij_worker import Task, TaskStatus


@unique
class DumpFormat(str, Enum):
    CYPHER_SHELL = "cypher-shell"
    GRAPHML = "graphml"


class DumpRequest(NoEnumModel, LowerCamelCaseModel):
    format: DumpFormat
    query: Optional[str] = None


class IncrementalImportRequest(LowerCamelCaseModel):
    query: Optional[Dict] = None
    # TODO: add all other parameters such as concurrency etc etc...


class IncrementalImportResponse(LowerCamelCaseModel):
    imported: int = 0
    nodes_created: int = 0
    relationships_created: int = 0


class CSVPaths:
    path: str


class Neo4jCSVRequest(LowerCamelCaseModel):
    query: Optional[Dict] = None


class NodeCSVs(LowerCamelCaseModel):
    labels: List[str]
    header_path: str
    node_paths: List[str]
    n_nodes: int


class RelationshipCSVs(LowerCamelCaseModel):
    types: List[str]
    header_path: str
    relationship_paths: List[str]
    n_relationships: int


class Neo4jCSVs(LowerCamelCaseModel):
    db: str
    nodes: List[NodeCSVs]
    relationships: List[RelationshipCSVs]


class Neo4jCSVResponse(LowerCamelCaseModel):
    path: str
    metadata: Neo4jCSVs


class TaskJob(LowerCamelCaseModel):
    type: str
    task_id: Optional[str] = None  # Used when the task_id is created by the client
    inputs: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None

    def to_task(self, task_id: Optional[str] = None) -> Task:
        job_id = self.task_id
        if job_id is None and task_id is None:
            msg = "task_id must be explicitly provided when incoming job has no task_id"
            raise ValueError(msg)
        created_at = self.created_at
        if created_at is None:
            created_at = datetime.now()
        return Task(
            id=task_id,
            type=self.type,
            inputs=self.inputs,
            status=TaskStatus.CREATED,
            created_at=created_at,
        )

    def generate_task_id(self) -> str:
        hashed = self.dict(by_alias=False)
        hashed.pop("created_at")
        hashed = hashlib.md5(json.dumps(hashed).encode(encoding="utf-8"))
        return f"{self.type}-{hashed.hexdigest()}"
