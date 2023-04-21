import asyncio
import functools
import io
import itertools
import json
import logging
import os
import sys
import tarfile
import tempfile
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, TextIO, Tuple

import neo4j
from datrie import BaseTrie

from neo4j_app import ROOT_DIR
from neo4j_app.constants import (
    DOC_COLUMNS,
    DOC_ID,
    DOC_ID_CSV,
    DOC_NODE,
    DOC_ROOT_REL_LABEL,
    NEO4J_CSV_COL,
    NEO4J_CSV_END_ID,
    NEO4J_CSV_ID,
    NEO4J_CSV_LABEL,
    NEO4J_CSV_START_ID,
    NEO4J_CSV_TYPE,
    NE_APPEARS_IN_DOC,
    NE_APPEARS_IN_DOC_COLS,
    NE_CATEGORY,
    NE_COLUMNS,
    NE_EXTRACTOR,
    NE_EXTRACTORS,
    NE_EXTRACTOR_LANG,
    NE_ID,
    NE_IDS,
    NE_MENTION_NORM,
    NE_NODE,
    NE_OFFSETS,
)
from neo4j_app.core.elasticsearch import ESClientABC
from neo4j_app.core.elasticsearch.client import PointInTime, sliced_search_with_pit
from neo4j_app.core.elasticsearch.to_neo4j import (
    es_to_neo4j_doc_csv,
    es_to_neo4j_doc_root_rel_csv,
    es_to_neo4j_doc_row,
    es_to_neo4j_named_entity_csv,
    es_to_neo4j_named_entity_doc_rel_csv,
    es_to_neo4j_named_entity_row,
    write_es_rows_to_ne_doc_rel_csv,
)
from neo4j_app.core.elasticsearch.utils import (
    ASC,
    ES_DOCUMENT_TYPE,
    ES_NAMED_ENTITY_TYPE,
    FIELD,
    HITS,
    ID_,
    KEEP_ALIVE,
    PIT,
    QUERY,
    SCORE_,
    SEED,
    SHARD_DOC_,
    SORT,
    and_query,
    function_score,
    has_id,
    has_parent,
    has_type,
)
from neo4j_app.core.neo4j import (
    Neo4Import,
    Neo4jImportWorker,
    get_neo4j_csv_reader,
    get_neo4j_csv_writer,
    write_neo4j_csv,
)
from neo4j_app.core.neo4j.documents import (
    documents_ids_tx,
    import_document_rows,
)
from neo4j_app.core.neo4j.named_entities import (
    ne_creation_stats_tx,
    import_named_entity_rows,
)
from neo4j_app.core.objects import (
    IncrementalImportResponse,
    Neo4jCSVResponse,
    Neo4jCSVs,
    NodeCSVs,
    RelationshipCSVs,
)
from neo4j_app.core.utils import batch
from neo4j_app.core.utils.asyncio import run_with_concurrency
from neo4j_app.core.utils.logging import log_elapsed_time_cm

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImportSummary:
    imported: int
    nodes_created: int
    relationships_created: int


class ImportTransactionFunction(Protocol):
    async def __call__(
        self,
        neo4j_session: neo4j.AsyncSession,
        *,
        batch_size: int,
        **kwargs,
    ) -> neo4j.ResultSummary:
        ...


async def import_documents(
    *,
    es_client: ESClientABC,
    es_query: Optional[Dict],
    es_concurrency: Optional[int] = None,
    es_keep_alive: Optional[str] = None,
    es_doc_type_field: str,
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_import_batch_size: int,
    neo4j_transaction_batch_size: int,
    max_records_in_memory: int,
) -> IncrementalImportResponse:
    es_query = _make_document_query(es_query, es_doc_type_field)
    if es_concurrency is None:
        es_concurrency = es_client.max_concurrency
    async with es_client.pit(keep_alive=es_keep_alive) as pit:
        # Since we're merging relationships we need to set the import concurrency to 1
        # to avoid deadlocks...
        neo4j_concurrency = 1
        bodies = [
            sliced_search_with_pit(
                es_query, pit=pit, id_=i, max_=es_concurrency, keep_alive=es_keep_alive
            )
            for i in range(es_concurrency)
        ]
        import_summary = await _es_to_neo4j_import(
            es_client=es_client,
            es_bodies=bodies,
            es_concurrency=es_concurrency,
            neo4j_driver=neo4j_driver,
            neo4j_concurrency=neo4j_concurrency,
            neo4j_import_batch_size=neo4j_import_batch_size,
            neo4j_transaction_batch_size=neo4j_transaction_batch_size,
            neo4j_import_fn=import_document_rows,
            to_neo4j_row=es_to_neo4j_doc_row,
            max_records_in_memory=max_records_in_memory,
            imported_entity_label="document nodes",
        )
    res = IncrementalImportResponse(
        imported=import_summary.imported,
        nodes_created=import_summary.nodes_created,
        relationships_created=import_summary.relationships_created,
    )
    return res


async def import_named_entities(
    *,
    es_client: ESClientABC,
    es_query: Optional[Dict],
    es_concurrency: Optional[int] = None,
    es_keep_alive: Optional[str] = None,
    es_doc_type_field: str,
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_import_batch_size: int,
    neo4j_transaction_batch_size: int,
    max_records_in_memory: int,
) -> IncrementalImportResponse:
    async with neo4j_driver.session() as neo4j_session:
        document_ids = await neo4j_session.execute_read(documents_ids_tx)
        # Because of this neo4j limitation (https://github.com/neo4j/neo4j/issues/13139)
        # we have to count the number of relation created manually
        initial_n_nodes, initial_n_rels = await neo4j_session.execute_read(
            ne_creation_stats_tx
        )
    # Since this is an incremental import we consider it reasonable to use an ES join,
    # however for named entities bulk import join should be avoided and post filtering
    # on the documentId will probably be much more efficient !
    # TODO: if joining is too slow, switch to post filtering
    # TODO: project document fields here in order to reduce the ES payloads...
    async with es_client.pit(keep_alive=es_keep_alive) as pit:
        pit[KEEP_ALIVE] = es_keep_alive
        neo4j_concurrency = 1
        bodies = _make_named_entity_with_parent_queries(
            es_query,
            document_ids=document_ids,
            es_pit=pit,
            es_doc_type_field=es_doc_type_field,
            es_page_size=es_client.pagination_size,
        )
        import_summary = await _es_to_neo4j_import(
            es_client=es_client,
            es_bodies=bodies,
            es_concurrency=es_concurrency,
            neo4j_driver=neo4j_driver,
            neo4j_concurrency=neo4j_concurrency,
            neo4j_import_batch_size=neo4j_import_batch_size,
            neo4j_transaction_batch_size=neo4j_transaction_batch_size,
            neo4j_import_fn=import_named_entity_rows,
            to_neo4j_row=es_to_neo4j_named_entity_row,
            max_records_in_memory=max_records_in_memory,
            imported_entity_label="named entity nodes",
        )
    async with neo4j_driver.session() as neo4j_session:
        n_nodes, n_rels = await neo4j_session.execute_read(ne_creation_stats_tx)
    res = IncrementalImportResponse(
        imported=import_summary.imported,
        nodes_created=n_nodes - initial_n_nodes,
        relationships_created=n_rels - initial_n_rels,
    )
    return res


def _make_neo4j_worker(
    name: str,
    neo4j_driver: neo4j.AsyncDriver,
    import_fn: Neo4Import,
    transaction_batch_size: int,
    to_neo4j_row: Callable[[Any], Dict],
) -> Neo4jImportWorker:
    return Neo4jImportWorker(
        name=name,
        neo4j_driver=neo4j_driver,
        import_fn=import_fn,
        transaction_batch_size=transaction_batch_size,
        to_neo4j=to_neo4j_row,
    )


async def _es_to_neo4j_import(
    *,
    es_client: ESClientABC,
    es_bodies: List[Mapping[str, Any]],
    es_concurrency: Optional[int] = None,
    neo4j_driver: neo4j.AsyncDriver,
    neo4j_concurrency: int,
    neo4j_import_fn: Neo4Import,
    neo4j_import_batch_size: int,
    neo4j_transaction_batch_size: int,
    to_neo4j_row: Callable[[Any], List[Dict]],
    max_records_in_memory: int,
    imported_entity_label: str,
) -> ImportSummary:
    neo4j_import_worker_factory = functools.partial(
        _make_neo4j_worker,
        neo4j_driver=neo4j_driver,
        import_fn=neo4j_import_fn,
        transaction_batch_size=neo4j_transaction_batch_size,
        to_neo4j_row=to_neo4j_row,
    )
    imported, summaries = await es_client.to_neo4j(
        es_bodies,
        neo4j_import_worker_factory=neo4j_import_worker_factory,
        num_neo4j_workers=neo4j_concurrency,
        import_batch_size=neo4j_import_batch_size,
        concurrency=es_concurrency,
        max_records_in_memory=max_records_in_memory,
        imported_entity_label=imported_entity_label,
    )
    nodes_created = sum(summary.counters.nodes_created for summary in summaries)
    relationships_created = sum(
        summary.counters.relationships_created for summary in summaries
    )
    summary = ImportSummary(
        imported=imported,
        nodes_created=nodes_created,
        relationships_created=relationships_created,
    )
    return summary


async def to_neo4j_csvs(
    *,
    export_dir: Path,
    es_query: Optional[Dict],
    es_client: ESClientABC,
    es_concurrency: Optional[int],
    es_keep_alive: Optional[str],
    es_doc_type_field: str,
) -> Neo4jCSVResponse:
    nodes = []
    relationships = []
    async with es_client.pit(keep_alive=es_keep_alive) as pit:
        doc_nodes_csvs, doc_rels_csvs = await _to_neo4j_doc_csvs(
            export_dir=export_dir,
            es_query=es_query,
            es_pit=pit,
            es_client=es_client,
            es_concurrency=es_concurrency,
            es_keep_alive=es_keep_alive,
            es_doc_type_field=es_doc_type_field,
        )
        nodes.append(doc_nodes_csvs)
        relationships.append(doc_rels_csvs)
        doc_nodes_header_path = export_dir.joinpath(doc_nodes_csvs.header_path)
        with doc_nodes_header_path.open() as f:
            doc_nodes_headers = get_neo4j_csv_reader(f).fieldnames
        doc_nodes_path = export_dir.joinpath(doc_nodes_csvs.node_paths[0])
        with doc_nodes_path.open() as f:
            reader = get_neo4j_csv_reader(f, fieldnames=doc_nodes_headers)
            document_ids = list(set(row[f"{DOC_ID}:{DOC_ID_CSV}"] for row in reader))
        ne_nodes_csvs, ne_rels_csvs = await _to_neo4j_ne_csvs(
            document_ids=document_ids,
            export_dir=export_dir,
            es_pit=pit,
            es_client=es_client,
            es_concurrency=es_concurrency,
            es_keep_alive=es_keep_alive,
            es_doc_type_field=es_doc_type_field,
        )
        nodes.append(ne_nodes_csvs)
        relationships.append(ne_rels_csvs)
    metadata = Neo4jCSVs(nodes=nodes, relationships=relationships)
    _, targz_path = tempfile.mkstemp(
        prefix="neo4j-export", suffix=".tar.gz", dir=export_dir
    )
    targz_path = Path(targz_path)
    _compress_csvs_destructively(export_dir, metadata, targz_path=targz_path)
    return Neo4jCSVResponse(path=str(targz_path), metadata=metadata)


_DOC_REL_END_CSV_COL = f"{NEO4J_CSV_END_ID}({DOC_NODE})"
_DOC_ROOT_REL_HEADER = [f"{NEO4J_CSV_START_ID}({DOC_NODE})", _DOC_REL_END_CSV_COL]


async def _to_neo4j_doc_csvs(
    *,
    export_dir: Path,
    es_query: Optional[Dict],
    es_pit: PointInTime,
    es_client: ESClientABC,
    es_concurrency: Optional[int],
    es_keep_alive: Optional[str],
    es_doc_type_field: str,
) -> Tuple[NodeCSVs, RelationshipCSVs]:
    doc_nodes_path = export_dir.joinpath("docs.csv")
    doc_nodes_header_path = export_dir.joinpath("docs-header.csv")
    doc_nodes_header, doc_nodes_mapping = _make_header_and_mapping(DOC_COLUMNS)
    doc_nodes_header.append(NEO4J_CSV_LABEL)
    with doc_nodes_header_path.open("w") as f:
        get_neo4j_csv_writer(f, doc_nodes_header).writeheader()
    doc_root_rel_path = export_dir.joinpath("doc-roots.csv")
    doc_root_rel_header_path = export_dir.joinpath("doc-roots-header.csv")

    with doc_root_rel_header_path.open("w") as f:
        get_neo4j_csv_writer(f, _DOC_ROOT_REL_HEADER).writeheader()
    es_query = _make_document_query(es_query, es_doc_type_field)
    logger.debug("Exporting document from ES with concurrency of %s", es_concurrency)
    with doc_nodes_path.open("w") as nodes_f:
        with doc_root_rel_path.open("w") as rels_f:
            with log_elapsed_time_cm(
                logger,
                logging.DEBUG,
                "Exported ES documents to neo4j csvs in {elapsed_time} !",
            ):
                to_neo4j_nodes = functools.partial(
                    es_to_neo4j_doc_csv, prop_to_col_header=doc_nodes_mapping
                )
                (
                    n_doc_nodes,
                    n_doc_rels,
                ) = await es_client.write_concurrently_neo4j_csvs(
                    es_query,
                    pit=es_pit,
                    nodes_f=nodes_f,
                    relationships_f=rels_f,
                    to_neo4j_nodes=to_neo4j_nodes,
                    to_neo4j_relationships=es_to_neo4j_doc_root_rel_csv,
                    nodes_header=doc_nodes_header,
                    relationships_header=_DOC_ROOT_REL_HEADER,
                    keep_alive=es_keep_alive,
                    concurrency=es_concurrency,
                )
    node = NodeCSVs(
        labels=[DOC_NODE],
        header_path=doc_nodes_header_path.name,
        node_paths=[doc_nodes_path.name],
        n_nodes=n_doc_nodes,
    )
    rel = RelationshipCSVs(
        types=[DOC_ROOT_REL_LABEL],
        header_path=doc_root_rel_header_path.name,
        relationship_paths=[doc_root_rel_path.name],
        n_relationships=n_doc_rels,
    )
    return node, rel


_NE_DOC_REL_START_CSV_COL = f"{NEO4J_CSV_START_ID}({NE_NODE})"
_NE_DOC_REL_HEADER = [_NE_DOC_REL_START_CSV_COL, _DOC_REL_END_CSV_COL, NEO4J_CSV_TYPE]


async def _to_neo4j_ne_csvs(
    *,
    document_ids: List[str],
    export_dir: Path,
    es_pit: PointInTime,
    es_client: ESClientABC,
    es_concurrency: Optional[int],
    es_keep_alive: Optional[str],
    es_doc_type_field: str,
):
    ne_nodes_path = export_dir.joinpath("entities.csv")
    ne_nodes_header_path = export_dir.joinpath("entities-header.csv")
    _, ne_nodes_col_to_csv_header = _make_header_and_mapping(NE_COLUMNS)
    ne_nodes_header = [NEO4J_CSV_ID, NE_MENTION_NORM, NEO4J_CSV_LABEL]
    with ne_nodes_header_path.open("w") as f:
        get_neo4j_csv_writer(f, ne_nodes_header).writeheader()
    ne_doc_rel_path = export_dir.joinpath("entity-docs.csv")
    ne_doc_rel_header_path = export_dir.joinpath("entity-docs-header.csv")
    ne_doc_rel_header, _ = _make_header_and_mapping(NE_APPEARS_IN_DOC_COLS)
    ne_doc_rel_header.extend(_NE_DOC_REL_HEADER)
    with ne_doc_rel_header_path.open("w") as f:
        get_neo4j_csv_writer(f, ne_doc_rel_header).writeheader()
    logger.debug(
        "Exporting named entities from ES with concurrency of %s", es_concurrency
    )
    with ne_nodes_path.open("w") as nodes_f:
        with ne_doc_rel_path.open("w") as rels_f:
            with log_elapsed_time_cm(
                logger,
                logging.DEBUG,
                "Exported ES named entities to neo4j csvs in {elapsed_time} !",
            ):
                n_ne_nodes, n_ne_rels = await _export_es_named_entities_as_csvs(
                    es_client=es_client,
                    document_ids=document_ids,
                    nodes_f=nodes_f,
                    relationships_f=rels_f,
                    nodes_header=ne_nodes_header,
                    relationships_header=ne_doc_rel_header,
                    nodes_col_to_csv_header=ne_nodes_col_to_csv_header,
                    es_pit=es_pit,
                    es_concurrency=es_concurrency,
                    es_keep_alive=es_keep_alive,
                    es_doc_type_field=es_doc_type_field,
                )
    node = NodeCSVs(
        labels=[],
        header_path=ne_nodes_header_path.name,
        node_paths=[ne_nodes_path.name],
        n_nodes=n_ne_nodes,
    )
    rel = RelationshipCSVs(
        types=[NE_APPEARS_IN_DOC],
        header_path=ne_doc_rel_header_path.name,
        relationship_paths=[ne_doc_rel_path.name],
        n_relationships=n_ne_rels,
    )
    return node, rel


async def _export_es_named_entities_as_csvs(
    es_client: ESClientABC,
    *,
    document_ids: List[str],
    nodes_f: TextIO,
    relationships_f: TextIO,
    nodes_header: List[str],
    relationships_header: List[str],
    nodes_col_to_csv_header: Dict[str, str],
    es_pit: PointInTime,
    es_concurrency: Optional[int],
    es_keep_alive: Optional[str],
    es_doc_type_field: str,
):
    if es_concurrency is None:
        es_concurrency = es_client.max_concurrency
    if es_keep_alive is None:
        es_keep_alive = es_client.keep_alive
    es_concurrency = max(es_concurrency, 2)
    # Max should be at least 2
    # We order results by doc id in order to be able to buffer in an efficient way
    # emptying the buffer when we find new document ids, we use the _shard_doc as a
    # tie breaker
    sort = [f"{SCORE_}:{ASC}", f"{SHARD_DOC_}:{ASC}"]
    # https://github.com/elastic/elasticsearch/issues/2917#issuecomment-239662433
    pit = deepcopy(es_pit)
    pit[KEEP_ALIVE] = es_keep_alive
    bodies = _make_named_entity_with_parent_queries(
        es_query=None,
        es_pit=pit,
        document_ids=document_ids,
        es_doc_type_field=es_doc_type_field,
        es_page_size=es_client.pagination_size,
        sort_by_doc_id=True,
    )
    lock = asyncio.Lock()
    # We maintain a list of existing NE "str((category, mentionNorm))" inside a Trie.
    # Since the number of NE can grow quickly, naive storing of
    # make_ne_hit_id(category, mentionNorm) -> (113bytes) could lead to sever GB of data
    # in memory. The Trie structure compresses the IDs into a prefix tree.
    alphabet = set(
        char for point in range(sys.maxunicode) for char in chr(point).lower()
    )
    seen_entities = BaseTrie(ranges=[(min(alphabet), max(alphabet))])
    es_to_neo4j_nodes = functools.partial(
        es_to_neo4j_named_entity_csv, prop_to_col_header=nodes_col_to_csv_header
    )
    tasks = (
        _aggregate_and_write_ne_nodes_and_relationships(
            es_client=es_client,
            nodes_f=nodes_f,
            relationships_f=relationships_f,
            es_to_neo4j_nodes=es_to_neo4j_nodes,
            es_to_neo4j_relationships=es_to_neo4j_named_entity_doc_rel_csv,
            nodes_header=nodes_header,
            relationships_header=relationships_header,
            seen_entities=seen_entities,
            sort=sort,
            lock=lock,
            body=body,
        )
        for body in bodies
    )
    res = [r async for r in run_with_concurrency(tasks, max_concurrency=es_concurrency)]
    res_nodes, res_rels = zip(*res)
    total_nodes = sum(res_nodes)
    total_rels = sum(res_rels)
    return total_nodes, total_rels


async def _aggregate_and_write_ne_nodes_and_relationships(
    es_client: ESClientABC,
    *,
    nodes_f: TextIO,
    relationships_f: TextIO,
    es_to_neo4j_nodes: Callable[[Dict], List[Dict[str, str]]],
    es_to_neo4j_relationships: Callable[[Dict], List[Dict[str, str]]],
    nodes_header: List[str],
    relationships_header: List[str],
    seen_entities: BaseTrie,
    lock: asyncio.Lock,
    **kwargs,
) -> Tuple[Optional[int], Optional[int]]:
    total_nodes = 0
    total_rels = 0
    max_seen_docs = es_client.pagination_size
    # We do an aggregation by docID, this is possible since each search is
    # processing different doc_ids batches AND the search is sorted by docID, so
    # we know that when the docID changes then we're done
    buffer = defaultdict(dict)
    current_doc_id = None
    seen_docs = set()
    async for res in es_client.poll_search_pages(**kwargs):
        ne_rows = [row for hit in res[HITS][HITS] for row in es_to_neo4j_nodes(hit)]
        ne_rels = [
            row for hit in res[HITS][HITS] for row in es_to_neo4j_relationships(hit)
        ]
        for rel in ne_rels:
            buffer = _fill_ne_aggregation_buffer(buffer, rel)
            doc_id = rel[_DOC_REL_END_CSV_COL]
            # Let's empty the buffer when we find a new document to avoid filling
            # memory
            if doc_id != current_doc_id:
                current_doc_id = doc_id
                seen_docs.add(current_doc_id)
                if len(seen_docs) >= max_seen_docs:
                    async with lock:
                        total_rels += len(buffer)
                        write_es_rows_to_ne_doc_rel_csv(
                            relationships_f,
                            rows=list(buffer.values()),
                            header=relationships_header,
                        )
                        relationships_f.flush()
                        buffer = defaultdict(dict)
                        seen_docs = set()
        async with lock:
            ne_with_keys = ((_ne_trie_key(ne), ne) for ne in ne_rows)
            new_rows = []
            for key, row in ne_with_keys:
                if key in seen_entities:
                    continue
                seen_entities[key] = 0
                new_rows.append(row)
            if new_rows:
                total_nodes += len(new_rows)
                write_neo4j_csv(
                    nodes_f, rows=new_rows, header=nodes_header, write_header=False
                )
                nodes_f.flush()
    if buffer:
        async with lock:
            total_rels += len(buffer)
            write_es_rows_to_ne_doc_rel_csv(
                relationships_f, rows=list(buffer.values()), header=relationships_header
            )
            relationships_f.flush()
    return total_nodes, total_rels


_NeDocRelationshipBuffer = Dict[Tuple[Tuple[str, str], str], Dict]


def _fill_ne_aggregation_buffer(
    buffer: _NeDocRelationshipBuffer, rel: Dict
) -> _NeDocRelationshipBuffer:
    key = ((rel[NE_MENTION_NORM], rel[NE_CATEGORY]), rel[_DOC_REL_END_CSV_COL])
    item = buffer[key]
    if NEO4J_CSV_TYPE not in item:
        item[NEO4J_CSV_TYPE] = NE_APPEARS_IN_DOC
    if _NE_DOC_REL_START_CSV_COL not in item:
        item[_NE_DOC_REL_START_CSV_COL] = rel[_NE_DOC_REL_START_CSV_COL]
    if _DOC_REL_END_CSV_COL not in item:
        item[_DOC_REL_END_CSV_COL] = rel[_DOC_REL_END_CSV_COL]
    if NE_EXTRACTOR_LANG not in item:
        item[NE_EXTRACTOR_LANG] = rel[NE_EXTRACTOR_LANG]
    if NE_IDS not in item:
        item[NE_IDS] = []
    item[NE_IDS].append(rel[NE_ID])
    if NE_EXTRACTORS not in item:
        item[NE_EXTRACTORS] = []
    item[NE_EXTRACTORS].append(rel[NE_EXTRACTOR])
    if NE_OFFSETS not in item:
        item[NE_OFFSETS] = []
    item[NE_OFFSETS].extend(rel[NE_OFFSETS])
    return buffer


def _make_document_query(es_query: Dict, es_doc_type_field: str) -> Dict:
    document_type_query = has_type(
        type_field=es_doc_type_field, type_value=ES_DOCUMENT_TYPE
    )
    if es_query is not None and es_query:
        es_query = and_query(document_type_query, es_query)
    else:
        es_query = {QUERY: document_type_query}
    return es_query


def _make_named_entity_query(
    es_query: Optional[Dict],
    *,
    es_doc_type_field: str,
):
    ne_type_query = has_type(
        type_field=es_doc_type_field, type_value=ES_NAMED_ENTITY_TYPE
    )
    if es_query is not None and es_query:
        es_query = and_query(ne_type_query, es_query)
    else:
        es_query = {QUERY: ne_type_query}
    return es_query


def _make_named_entity_with_parent_queries(
    es_query: Optional[Dict],
    *,
    es_sort: Optional[List[Dict]] = None,
    es_pit: PointInTime,
    document_ids: List[str],
    es_doc_type_field: str,
    es_page_size: int,
    sort_by_doc_id: bool = False,
) -> List[Mapping[str, str]]:
    # document_ids // es_concurrency might still too large to send over the network, on
    # the other hand changing of document_ids at each query won't benefit from ES
    # caching we hence arbitrarily choose perform the same query 10 times in a row to
    # benefit from caching while 10 * page_size IDs will be OK to send over the network
    batch_size = 10 * es_page_size
    doc_typ_query = has_type(
        type_field=es_doc_type_field, type_value=ES_NAMED_ENTITY_TYPE
    )
    bodies = []
    for doc_ids_batch in batch(document_ids, batch_size):
        has_id_query = has_id(doc_ids_batch)
        score = False
        if sort_by_doc_id:
            # When we want to group ne by docId, we set a random score for each doc
            # this way ne from the same doc will be grouped together
            score = True
            has_id_query = function_score(
                query=has_id_query, random_score={SEED: 0, FIELD: ID_}
            )
        queries = [
            doc_typ_query,
            has_parent(parent_type=ES_DOCUMENT_TYPE, query=has_id_query, score=score),
        ]
        if es_query is not None and es_query:
            queries.append(es_query)
        query = and_query(*queries)
        query[PIT] = es_pit
        if es_sort:
            query[SORT] = es_sort
        bodies.append(query)
    return bodies


def _ne_trie_key(ne: Dict) -> str:
    return str((ne[NEO4J_CSV_LABEL].lower(), ne[NE_MENTION_NORM]))


def _make_header_and_mapping(
    column_description: Dict[str, Dict]
) -> Tuple[List[str], Dict[str, str]]:
    header = []
    mapping = dict()
    for col, types in column_description.items():
        col_header = col
        conversion_type = types.get(NEO4J_CSV_COL)
        if conversion_type is not None:
            col_header += f":{conversion_type}"
        header.append(col_header)
        mapping[col] = col_header
    return header, mapping


def _compress_csvs_destructively(
    export_dir: Path, metadata: Neo4jCSVs, *, targz_path: Path
):
    with tarfile.open(targz_path, "w:gz") as tar:
        # Index
        json_index = json.dumps(metadata.dict()).encode()
        index = io.BytesIO(json_index)
        index_info = tarfile.TarInfo(name="metadata.json")
        index_info.size = len(json_index)
        tar.addfile(index_info, index)
        # Import script
        tar.add(
            ROOT_DIR.joinpath("scripts", "bulk-import.sh"), arcname="bulk-import.sh"
        )
        # CSVs
        nodes = (p for nodes in metadata.nodes for p in nodes.node_paths)
        relationships = (
            p for rels in metadata.relationships for p in rels.relationship_paths
        )
        to_compress = itertools.chain(nodes, relationships)
        for path in to_compress:
            _compress_and_destroy(
                tar, root_dir=export_dir, path=export_dir.joinpath(path)
            )
    return targz_path


def _compress_and_destroy(tar: tarfile.TarFile, *, root_dir: Path, path: Path):
    tar.add(path, arcname=str(path.relative_to(root_dir)))
    os.remove(path)
