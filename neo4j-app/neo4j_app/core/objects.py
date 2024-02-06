from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum, unique
from typing import Any, ClassVar, Dict, List, Optional, Union

import neo4j
from pydantic import Field

from neo4j_app.constants import STATS_ID, STATS_NODE, STATS_N_DOCS, STATS_N_ENTS
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


class GraphCounts(LowerCamelCaseModel):
    documents: int = 0
    named_entities: Dict[str, int] = Field(default_factory=dict)


class ProjectStatistics(LowerCamelCaseModel):
    singleton_stat_id: ClassVar[str] = Field(
        default="project-stats-singleton-id", const=True
    )
    counts: GraphCounts = Field(default_factory=GraphCounts)

    @classmethod
    async def from_neo4j(cls, tx: neo4j.AsyncTransaction) -> ProjectStatistics:
        query = f"MATCH (stats:{STATS_NODE}) RETURN *"
        stats_res = await tx.run(query)
        stats = [s async for s in stats_res]
        if not stats:
            return ProjectStatistics()
        if len(stats) > 1:
            raise ValueError("Inconsistent state, found several project statistics")
        stats = stats[0]["stats"]
        ent_counts_as_list = stats[STATS_N_ENTS]
        ent_counts = dict()
        for ent_ix in range(0, len(ent_counts_as_list), 2):
            ent_count_ix = ent_ix + 1
            ent_counts[ent_counts_as_list[ent_ix]] = int(
                ent_counts_as_list[ent_count_ix]
            )
        counts = GraphCounts(documents=stats[STATS_N_DOCS], named_entities=ent_counts)
        return ProjectStatistics(counts=counts)

    @classmethod
    async def to_neo4j_tx(
        cls, tx: neo4j.AsyncTransaction, doc_count: int, ent_counts: Dict[str, int]
    ) -> ProjectStatistics:
        query = f"""MERGE (s:{STATS_NODE} {{ {STATS_ID}: $singletonId }})
SET s.{STATS_N_DOCS} = $docCount, s.{STATS_N_ENTS} = $entCounts"""
        ent_counts_as_list = [
            entry for k, v in ent_counts.items() for entry in (k, str(v))
        ]
        await tx.run(
            query,
            singletonId=cls.singleton_stat_id.default,
            docCount=doc_count,
            entCounts=ent_counts_as_list,
        )
        counts = GraphCounts(documents=doc_count, named_entities=ent_counts)
        return cls(counts=counts)


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
