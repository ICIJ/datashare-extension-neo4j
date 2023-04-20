from __future__ import annotations

from typing import Dict, List, Optional

from neo4j_app.core.utils.pydantic import LowerCamelCaseModel


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
    export_dir: str
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
    nodes: List[NodeCSVs]
    relationships: List[RelationshipCSVs]


class Neo4jCSVResponse(LowerCamelCaseModel):
    path: str
    metadata: Neo4jCSVs
