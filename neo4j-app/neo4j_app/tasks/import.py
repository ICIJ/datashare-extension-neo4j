import logging

from neo4j_app.app.dependencies import (
    lifespan_config,
    lifespan_es_client,
    lifespan_neo4j_driver,
)
from neo4j_app.core.imports import import_documents, import_named_entities
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.core.utils.logging import log_elapsed_time_cm
from neo4j_app.core.utils.progress import scaled_progress
from neo4j_app.core.utils.pydantic import LowerCamelCaseModel
from neo4j_app.typing_ import PercentProgress
from . import app

logger = logging.getLogger(__name__)


class FullImportResponse(LowerCamelCaseModel):
    documents: IncrementalImportResponse
    named_entities: IncrementalImportResponse


@app
async def full_import(project: str, progress: PercentProgress) -> FullImportResponse:
    # Ideally we'd like to restrict the named entities
    doc_import_max_progress = 5.0  # This is a bit arbitrary...
    progress = scaled_progress(progress, end=doc_import_max_progress)
    config = lifespan_config()
    with log_elapsed_time_cm(
        logger, logging.INFO, "Imported documents in {elapsed_time} !"
    ):
        es_client = lifespan_es_client()
        neo4j_driver = lifespan_neo4j_driver()
        doc_res = await import_documents(
            project=project,
            es_client=es_client,
            es_query=None,
            es_concurrency=es_client.max_concurrency,
            es_keep_alive=config.es_keep_alive,
            es_doc_type_field=config.es_doc_type_field,
            neo4j_driver=neo4j_driver,
            neo4j_import_batch_size=config.neo4j_import_batch_size,
            neo4j_transaction_batch_size=config.neo4j_transaction_batch_size,
            max_records_in_memory=config.neo4j_app_max_records_in_memory,
            progress=progress,
        )
    logger.info("imported documents: %s", doc_res.json(sort_keys=True))
    progress = scaled_progress(progress, start=doc_import_max_progress)
    with log_elapsed_time_cm(
        logger, logging.INFO, "Imported named entities in {elapsed_time} !"
    ):
        ne_res = await import_named_entities(
            project=project,
            es_client=es_client,
            es_query=None,
            es_concurrency=es_client.max_concurrency,
            es_keep_alive=config.es_keep_alive,
            es_doc_type_field=config.es_doc_type_field,
            neo4j_driver=neo4j_driver,
            neo4j_import_batch_size=config.neo4j_import_batch_size,
            neo4j_transaction_batch_size=config.neo4j_transaction_batch_size,
            max_records_in_memory=config.neo4j_app_max_records_in_memory,
            progress=progress,
        )
    logger.info("imported documents: %s", doc_res.json(sort_keys=True))
    await progress(100.0)
    return FullImportResponse(documents=doc_res, named_entities=ne_res)
