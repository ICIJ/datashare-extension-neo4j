from typing import Dict, Optional

from neo4j_app.core.utils.pydantic import LowerCamelCaseModel


class DocumentImportRequest(LowerCamelCaseModel):
    query: Optional[Dict] = None


class DocumentImportResponse(LowerCamelCaseModel):
    n_docs_to_insert: int
    n_inserted_docs: int
