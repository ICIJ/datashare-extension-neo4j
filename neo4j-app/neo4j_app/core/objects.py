from neo4j_app.core.utils.pydantic import LowerCamelCaseModel


class DocumentImportResponse(LowerCamelCaseModel):
    n_to_insert: int
    n_inserted: int
