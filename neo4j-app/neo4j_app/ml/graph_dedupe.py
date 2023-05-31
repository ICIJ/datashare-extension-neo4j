from __future__ import annotations

import collections
import csv
import itertools
import random
import sqlite3
import tempfile
from typing import BinaryIO, Collection, Dict, Iterable, Set, TextIO, overload

import sklearn
from dedupe import blocking, core, training
from dedupe._typing import (
    ComparisonCoverInt,
    ComparisonCoverStr,
    Data,
    DataInt,
    DataStr,
    FeaturizerFunction,
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


# TODO: change the naming from DocumentGraph to HardBlockingDedupe or something like
#  this


def read_records(dataset_f: TextIO, id_column: str, invalid_ids: Set[str]) -> Dict:
    reader = csv.DictReader(dataset_f)
    records = {
        row[id_column]: row for row in reader if row[id_column] not in invalid_ids
    }
    return records


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

    def most_uncertain_pairs(self, n: int) -> RecordPairs:
        assert (
            self.active_learner is not None
        ), "Please initialize with the prepare_training method"
        return self.active_learner.pop_n(n=n)


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

        self.matcher = MatchLearner(featurizer, self.candidates)

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
