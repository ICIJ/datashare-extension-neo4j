from __future__ import annotations

import collections
import csv
import itertools
import random
import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import (
    AsyncIterable,
    BinaryIO,
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    TextIO,
    overload,
)

import sklearn
from dedupe import blocking, core, read_training, training
from dedupe._typing import (
    ComparisonCoverInt,
    ComparisonCoverStr,
    Data,
    DataInt,
    DataStr,
    FeaturizerFunction,
    RecordDict,
    RecordPairs,
    VariableDefinition,
)
from dedupe.api import Dedupe, DedupeMatching, StaticMatching, flatten_training
from dedupe.labeler import (
    BlockLearner,
    DedupeBlockLearner,
    DisagreementLearner,
    MatchLearner,
    sample_records,
)
from dedupe.predicates import Predicate

from neo4j_app.constants import (
    DOC_CONTENT_TYPE,
    DOC_DIRNAME,
    DOC_ID,
    DOC_ROOT_ID,
    NE_MENTION_NORM,
)
from neo4j_app.core.utils.pydantic import to_lower_camel
from neo4j_app.ml.utils import filtering_console_label

NE_DOC_ID = to_lower_camel(f"doc_{DOC_ID}")
NE_DOC_DIR_NAME = to_lower_camel(f"doc_{DOC_DIRNAME}")
NE_DOC_FILENAME = "docFilename"
NE_DOC_CONTENT_TYPE = to_lower_camel(f"doc_{DOC_CONTENT_TYPE}")
NE_DOC_ROOT_ID = to_lower_camel(f"doc_{DOC_ROOT_ID}")
NE_DEBUG_DOC_URL = "debugDocUrl"
NE_DEBUG_FILENAME = "debugFilename"
NE_MENTION_CLUSTER = "neMentionClusterID"
# TODO: fix this naming..
NE_MENTION_NORM_DOC_ID = "neMentionNormDocID"

NE_FIELDNAMES = [
    NE_MENTION_NORM,
    NE_DOC_ID,
    NE_DOC_DIR_NAME,
    NE_DOC_FILENAME,
    NE_DOC_CONTENT_TYPE,
    NE_DOC_ROOT_ID,
    NE_DEBUG_DOC_URL,
    NE_DEBUG_FILENAME,
]


async def async_write_dataset(
    records: AsyncIterable[Dict], fieldnames: List[str], dataset_f: TextIO
):
    writer = csv.DictWriter(dataset_f, fieldnames)
    writer.writeheader()
    async for rec in records:
        writer.writerow(rec)


def write_dataset(records: Iterable[Dict], fieldnames: List[str], dataset_f: TextIO):
    writer = csv.DictWriter(dataset_f, fieldnames)
    writer.writeheader()
    for rec in records:
        writer.writerow(rec)


def read_records(
    dataset_f: TextIO, id_column: str, invalid_ids: Set[str]
) -> Dict[str, RecordDict]:
    reader = csv.DictReader(dataset_f)
    records = {
        row[id_column]: row for row in reader if row[id_column] not in invalid_ids
    }
    return records


@contextmanager
def yield_none():
    yield None


def get_categories(records: Iterable[RecordDict]) -> Set[str]:
    category = set(rec[NE_DOC_CONTENT_TYPE] for rec in records)
    return category


def get_mentions(records: Iterable[RecordDict]) -> Set[str]:
    filenames = set(rec[NE_MENTION_NORM] for rec in records)
    return filenames


def get_dirnames(records: Iterable[RecordDict]) -> Set[str]:
    dirname = set(rec[NE_DOC_DIR_NAME] for rec in records)
    return dirname


def person_fields(records: Iterable[RecordDict], inside_docs: bool) -> List[Dict]:
    # TODO: add the number of documents of the same type in the doc as feature ?

    # TODO: check that there a not duplicate predicates
    records = list(records)
    fields = [
        # Use both string and text type for mention norm to capture char-level (String)
        # and words (Text) similarities and differences
        {"field": NE_MENTION_NORM, "type": "String"},
        {"field": NE_MENTION_NORM, "type": "Text", "corpus": get_mentions(records)},
        {"field": NE_MENTION_NORM, "type": "Person Name"},
    ]
    if not inside_docs:
        cross_doc_fields = [
            # Exact match on IDs
            {"field": NE_DOC_ID, "type": "Exact"},
            {"field": NE_DOC_ROOT_ID, "type": "Exact", "has missing": True},
            # Finite set of values for categories
            {
                "field": NE_DOC_CONTENT_TYPE,
                "type": "Categorical",
                "categories": get_categories(records),
            },
            # We hope that some file names will have word level similarities
            {"field": NE_DOC_FILENAME, "type": "Exact"},
            # We hope that some file names will have word level similarities
            {"field": NE_DOC_DIR_NAME, "type": "Text", "corpus": get_dirnames(records)},
            {"field": NE_DOC_DIR_NAME, "type": "Exact"},
            {"field": NE_MENTION_CLUSTER, "type": "Exact"},
        ]
        fields.extend(cross_doc_fields)
    return fields


def run_training(
    records: Data,
    *,
    dedupe_getter: Callable[[List[Dict]], Dedupe],
    fields_getter: Callable[[Iterable[RecordDict]], List[Dict]],
    excluded_path: Path,
    model_path: Path,
    training_path: Path,
    sample_size: int,
    id_column: str,
    recall: float,
) -> Dedupe:
    # TODO: this function has too many IO... IO should be separate from the core...
    with excluded_path.open() as f:
        invalid_ids = (line.strip() for line in f)
        invalid_ids = set(i for i in invalid_ids if i)

    all_records = records.values()
    training_file_cm = yield_none
    if training_path.exists():
        training_file_cm = training_path.open
        with training_file_cm() as training_file:
            training_set = read_training(training_file)
            all_records_its = [
                all_records,
                (r for pair in training_set["distinct"] for r in pair),
                (r for pair in training_set["match"] for r in pair),
            ]
            all_records = itertools.chain(*all_records_its)

    with training_file_cm() as training_file:
        # TODO: clean this... we should have to reopen the training file
        fields = fields_getter(all_records)
        deduper = dedupe_getter(variable_definition=fields)
        deduper.prepare_training(records, training_file, sample_size)
        clf_args = getattr(deduper, "clf_args")
        if clf_args is not None:
            # TODO: clean this mess make the matcher configurable
            learner = deduper.active_learner
            learner.matcher = ConfigurableMatchLearner(
                deduper.data_model.distances, learner.candidates, **clf_args
            )
            learner.matcher.fit(learner.pairs, learner.y)

    invalid = filtering_console_label(deduper, id_column=id_column)
    invalid_ids.update((rec[id_column] for rec in invalid))
    excluded_path.write_text("\n".join(invalid_ids))

    with training_path.open("w") as f:
        deduper.write_training(f)

    deduper.train(recall=recall)
    with model_path.open("wb") as f:
        deduper.write_settings(f)

    return deduper


class ConfigurableMatchLearner(MatchLearner):
    def __init__(
        self, featurizer: FeaturizerFunction, candidates: RecordPairs, **clf_args
    ):
        super().__init__(featurizer, candidates)
        self.clf_args = clf_args
        self._classifier = sklearn.linear_model.LogisticRegression(**clf_args)


class ConfigurableClassifierDedupe(Dedupe):
    def __init__(
        self,
        variable_definition: Collection[VariableDefinition],
        num_cores: int | None = None,
        in_memory: bool = False,
        clf_args: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(
            variable_definition=variable_definition,
            num_cores=num_cores,
            in_memory=in_memory,
            **kwargs,
        )
        if clf_args is None:
            clf_args = dict()
        self.clf_args = clf_args
        self.classifier = sklearn.model_selection.GridSearchCV(
            estimator=sklearn.linear_model.LogisticRegression(**self.clf_args),
            param_grid={"C": [0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10]},
            scoring="f1",
            n_jobs=-1,
        )


# TODO: change the naming from DocumentGraph to HardBlockingDedupe or something like
#  this


class DocumentGraphDedupe(Dedupe):
    def __init__(
        self,
        doc_key: str,
        variable_definition: Collection[VariableDefinition],
        num_cores: int | None = None,
        in_memory: bool = False,
        **kwargs,
    ):
        super().__init__(
            variable_definition=variable_definition,
            num_cores=num_cores,
            in_memory=in_memory,
            **kwargs,
        )
        self.classifier = sklearn.model_selection.GridSearchCV(
            estimator=sklearn.linear_model.LogisticRegression(max_iter=1000),
            param_grid={"C": [0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10]},
            scoring="f1",
            n_jobs=-1,
        )
        self._doc_key = doc_key

    def prepare_training(
        self,
        data: Data,
        training_file: TextIO | None = None,
        sample_size: int = 1500,
        blocked_proportion: float = 0.9,
    ) -> None:
        self._checkData(data)

        # Reset active learner
        self.active_learner = None

        if training_file:
            self._read_training(training_file)

        # We need the active learner to know about all our
        # existing training data, so add them to data dictionary
        examples, y = flatten_training(self.training_pairs)

        self.active_learner = DocumentGraphDedupeDisagreementLearner(
            self._doc_key,
            self.data_model.predicates,
            self.data_model.distances,
            data,
            index_include=examples,
        )

        self.active_learner.mark(examples, y)


class DocumentGraphDedupeDisagreementLearner(DisagreementLearner):
    def __init__(
        self,
        doc_key: str,
        candidate_predicates: Iterable[Predicate],
        featurizer: FeaturizerFunction,
        data: Data,
        index_include: RecordPairs,
    ):
        super().__init__()
        self._doc_key = doc_key
        data = core.index(data)

        random_pair = (
            random.choice(list(data.values())),
            random.choice(list(data.values())),
        )
        exact_match = (random_pair[0], random_pair[0])

        index_include = index_include.copy()
        index_include.append(exact_match)

        self.blocker = DocumentGraphDedupeBlockLearner(
            self._doc_key, candidate_predicates, data, index_include
        )

        self._candidates = self.blocker.candidates.copy()

        self.matcher = ConfigurableMatchLearner(
            featurizer, self.candidates, max_iter=10000
        )

        examples = [exact_match] * 4 + [random_pair]
        labels: Labels = [1] * 4 + [0]  # type: ignore[assignment]
        self.mark(examples, labels)


def _filter_canopy_predicates(
    predicates: Iterable[Predicate], canopies: bool
) -> set[Predicate]:
    result = set()
    for predicate in predicates:
        if hasattr(predicate, "index"):
            is_canopy = hasattr(predicate, "canopy")
            if is_canopy == canopies:
                result.add(predicate)
        else:
            result.add(predicate)
    return result


class DocumentGraphDedupeBlockLearner(DedupeBlockLearner):
    def __init__(
        self,
        doc_key: str,
        candidate_predicates: Iterable[Predicate],
        data: Data,
        index_include: RecordPairs,
    ):
        BlockLearner.__init__(self)
        self._doc_key = doc_key

        N_SAMPLED_RECORDS = 5000
        N_SAMPLED_RECORD_PAIRS = 10000

        index_data = sample_records(data, 50000)
        sampled_records = sample_records(index_data, N_SAMPLED_RECORDS)

        preds = _filter_canopy_predicates(candidate_predicates, canopies=True)
        self.block_learner = DocumentGraphDedupeTrainingBlockLearner(
            self._doc_key, preds, sampled_records, index_data
        )

        self._candidates = self._sample(sampled_records, N_SAMPLED_RECORD_PAIRS)
        examples_to_index = self.candidates.copy()

        if index_include:
            examples_to_index += index_include

        self._index_predicates(examples_to_index)


class DocumentGraphDedupeTrainingBlockLearner(training.BlockLearner):
    def __init__(
        self,
        doc_key: str,
        predicates: Iterable[Predicate],
        sampled_records: Data,
        data: Data,
    ):
        self._doc_key = doc_key
        self.blocker = blocking.Fingerprinter(predicates)
        self.blocker.index_all(data)

        self.comparison_cover = self.coveredPairs(
            self.blocker, sampled_records, self._doc_key
        )

    @overload
    @staticmethod
    def coveredPairs(
        blocker: blocking.Fingerprinter, records: DataInt, doc_key: str
    ) -> ComparisonCoverInt:
        ...

    @overload
    @staticmethod
    def coveredPairs(
        blocker: blocking.Fingerprinter, records: DataStr, doc_key: str
    ) -> ComparisonCoverStr:
        ...

    @staticmethod
    def coveredPairs(blocker: blocking.Fingerprinter, records, doc_key: str):
        cover = {}

        n_records = len(records)

        for predicate in blocker.predicates:
            pred_cover = collections.defaultdict(set)

            for id, record in records.items():
                blocks = predicate(record)
                for block in blocks:
                    pred_cover[block].add(id)

            if not pred_cover:
                continue

            max_cover = max(len(v) for v in pred_cover.values())
            if max_cover == n_records:
                continue

            pairs = frozenset(
                pair
                for block in pred_cover.values()
                for pair in itertools.combinations(sorted(block), 2)
                if records[pair[0]][doc_key] == records[pair[1]][doc_key]
            )

            if pairs:
                cover[predicate] = pairs

        return cover


class HardDedupeMatching(DedupeMatching):
    _doc_key: str

    def pairs(self, data: Data) -> RecordPairs:
        self.fingerprinter.index_all(data)

        id_type = core.sqlite_id_type(data)

        # Blocking and pair generation are typically the first memory
        # bottlenecks, so we'll use sqlite3 to avoid doing them in memory
        with tempfile.TemporaryDirectory() as temp_dir:
            if self.in_memory:
                con = sqlite3.connect(":memory:")
            else:
                con = sqlite3.connect(temp_dir + "/blocks.db")

            # Set journal mode to WAL.
            con.execute("pragma journal_mode=off")
            con.execute(
                f"CREATE TABLE blocking_map (block_key text, record_id {id_type})"
            )
            con.executemany(
                "INSERT INTO blocking_map values (?, ?)",
                self.fingerprinter(data.items()),
            )

            self.fingerprinter.reset_indices()

            con.execute(
                """CREATE UNIQUE INDEX record_id_block_key_idx
                           ON blocking_map (record_id, block_key)"""
            )
            con.execute(
                """CREATE INDEX block_key_idx
                           ON blocking_map (block_key)"""
            )
            con.execute("""ANALYZE""")
            pairs = con.execute(
                """SELECT DISTINCT a.record_id, b.record_id
                                   FROM blocking_map a
                                   INNER JOIN blocking_map b
                                   USING (block_key)
                                   WHERE a.record_id < b.record_id"""
            )

            for a_record_id, b_record_id in pairs:
                record_a = data[a_record_id]
                record_b = data[b_record_id]
                if record_a[self._doc_key] != record_b[self._doc_key]:
                    continue
                yield ((a_record_id, record_a), (b_record_id, record_b))

            pairs.close()
            con.close()


class HardStaticDedupe(StaticMatching, HardDedupeMatching):
    def __init__(
        self,
        doc_key: str,
        settings_file: BinaryIO,
        num_cores: int | None = None,
        in_memory: bool = False,
        **kwargs,
    ) -> None:
        self._doc_key = doc_key
        super().__init__(settings_file, num_cores, in_memory, **kwargs)


def compute_membership(
    partition: List, *, cluster_key: str, confidence_key: str
) -> Dict:
    membership = dict()
    for cluster_id, (records, scores) in enumerate(partition):
        for record_id, score in zip(records, scores):
            membership[record_id] = {
                cluster_key: cluster_id,
                confidence_key: float(score),
            }
    return membership
