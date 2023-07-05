from __future__ import annotations

from enum import Enum, unique
from typing import Dict, List, Optional

from pydantic import Field

from neo4j_app.core.utils.pydantic import LowerCamelCaseModel, NoEnumModel


@unique
class DumpFormat(str, Enum):
    CYPHER_SHELL = "cypher-shell"
    GRAPHML = "graphml"


class DumpRequest(NoEnumModel, LowerCamelCaseModel):
    format: DumpFormat
    query: Optional[str] = None


class GraphSchema(LowerCamelCaseModel):
    nodes: List[NodeSchema] = Field(default_factory=list)
    relationships: List[RelationshipSchema] = Field(default_factory=list)


class IncrementalImportRequest(LowerCamelCaseModel):
    query: Optional[Dict] = None
    # TODO: add all other parameters such as concurrency etc etc...


class IncrementalImportResponse(LowerCamelCaseModel):
    imported: int = 0
    nodes_created: int = 0
    relationships_created: int = 0


class Neo4jCSVs(LowerCamelCaseModel):
    db: str
    nodes: List[NodeCSVs]
    relationships: List[RelationshipCSVs]


class Neo4jCSVResponse(LowerCamelCaseModel):
    path: str
    metadata: Neo4jCSVs


class Neo4jCSVRequest(LowerCamelCaseModel):
    query: Optional[Dict] = None


class NodeCSVs(LowerCamelCaseModel):
    labels: List[str]
    header_path: str
    node_paths: List[str]
    n_nodes: int


class Neo4jProperty(LowerCamelCaseModel):
    name: str
    types: List[str]
    mandatory: bool


class NodeSchema(LowerCamelCaseModel):
    label: str
    properties: List[Neo4jProperty]


class RelationshipCSVs(LowerCamelCaseModel):
    types: List[str]
    header_path: str
    relationship_paths: List[str]
    n_relationships: int


class RelationshipSchema(LowerCamelCaseModel):
    type: str
    properties: List[Neo4jProperty]


GraphSchema.update_forward_refs()
