from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum, unique
from typing import Any, Dict, List, Optional, Union

import neo4j
from pydantic import Field

from neo4j_app.core.utils.pydantic import LowerCamelCaseModel, NoEnumModel
from neo4j_app.icij_worker.task import Task, TaskStatus


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


class GraphNodesCount(LowerCamelCaseModel):
    documents: int = 0
    named_entities: Dict[str, int] = Field(default_factory=dict)

    @classmethod
    async def from_neo4j(
        cls,
        *,
        doc_res: neo4j.AsyncResult,
        entity_res: neo4j.AsyncResult,
        document_key="nDocs",
        entity_labels_key="neLabels",
        entity_count_key="nMentions",
    ) -> GraphNodesCount:
        doc_res = await doc_res.single()
        n_docs = doc_res[document_key]
        n_ents = dict()
        async for rec in entity_res:
            labels = rec[entity_labels_key]
            # This might require to fix admin imports to create distinct nodes
            if len(labels) != 1:
                msg = (
                    "Expected named entity to have exactly 2 labels."
                    " Refactor this function."
                )
                raise ValueError(msg)
            n_ents[labels[0]] = rec[entity_count_key]
        return GraphNodesCount(documents=n_docs, named_entities=n_ents)


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
    task_type: str
    task_id: Optional[str] = None  # Used when the task_id is created by the client
    inputs: Optional[Dict[str, Any]] = None
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
            type=self.task_type,
            inputs=self.inputs,
            status=TaskStatus.CREATED,
            created_at=created_at,
        )

    def generate_task_id(self) -> str:
        hashed = self.dict(by_alias=False)
        hashed.pop("created_at")
        hashed = hashlib.md5(json.dumps(hashed).encode(encoding="utf-8"))
        return f"{self.task_type}-{hashed.hexdigest()}"


class TaskSearch(LowerCamelCaseModel):
    type: Optional[str] = None
    status: Optional[Union[List[TaskStatus], TaskStatus]] = None
