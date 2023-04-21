from typing import Dict, Optional

from neo4j_app.core.utils.pydantic import LowerCamelCaseModel


class IncrementalImportRequest(LowerCamelCaseModel):
    query: Optional[Dict] = None
    # TODO: add all other parameters such as concurrency etc etc...


class IncrementalImportResponse(LowerCamelCaseModel):
    imported: int = 0
    nodes_created: int = 0
    relationships_created: int = 0
