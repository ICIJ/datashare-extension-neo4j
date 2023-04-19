from __future__ import annotations

import os
import tarfile
from pathlib import Path
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

    def gz(self, root_dir: Path, destructively: bool) -> NodeCSVs:
        node_paths = []
        for path in self.node_paths:
            rel_path = root_dir.joinpath(path)
            node_paths.append(_tar_gz(rel_path).name)
            if destructively:
                os.remove(rel_path)
        return self.copy(update={"node_paths": node_paths}, deep=True)


class RelationshipCSVs(LowerCamelCaseModel):
    types: List[str]
    header_path: str
    relationship_paths: List[str]
    n_relationships: int

    def gz(self, root_dir: Path, destructively: bool) -> RelationshipCSVs:
        relationship_paths = []
        for path in self.relationship_paths:
            rel_path = root_dir.joinpath(path)
            relationship_paths.append(_tar_gz(rel_path).name)
            if destructively:
                os.remove(rel_path)
        return self.copy(update={"relationship_paths": relationship_paths}, deep=True)


class Neo4jCSVResponse(LowerCamelCaseModel):
    nodes: List[NodeCSVs]
    relationships: List[RelationshipCSVs]

    def gz(self, root_dir: Path, destructively: bool) -> Neo4jCSVResponse:
        new_nodes = [n.gz(root_dir, destructively) for n in self.nodes]
        new_relationships = [r.gz(root_dir, destructively) for r in self.relationships]
        self_gz = self.copy(
            update={"nodes": new_nodes, "relationships": new_relationships}, deep=True
        )
        return self_gz


def _tar_gz(path: Path) -> Path:
    targz_path = path.with_name(path.name + ".gz")
    with tarfile.open(targz_path, "w:gz") as tar:
        tar.add(path, arcname=path.name)
    return targz_path
