import sys
from typing import Dict, List

import dedupe
from dedupe._typing import RecordDictPair, TrainingData
from dedupe.api import ActiveMatching
from dedupe.convenience import LabeledPair
from dedupe.core import unique


def _print(*args) -> None:
    print(*args, file=sys.stderr)


def _mark_pairs(
    deduper: dedupe.api.ActiveMatching, labeled_pairs: List[LabeledPair]
) -> None:
    examples: TrainingData = {"distinct": [], "match": []}
    for pair in labeled_pairs:
        record_pair, label = pair
        if label == "unsure":
            # See https://github.com/dedupeio/dedupe/issues/984 for reasoning
            examples["match"].append(record_pair)
            examples["distinct"].append(record_pair)
        else:
            # label is either "match" or "distinct"
            examples[label].append(record_pair)
    deduper.mark_pairs(examples)


_BASE_PROMPT = (
    "(y)es / (n)o / (u)nsure / (l)eft invalid / (r)ight invalid /"
    " (b)oth invalid / (f)inished"
)
_WITH_PREVIOUS_PROMPT = f"{_BASE_PROMPT} / (p)revious"


def filtering_console_label(
    deduper: ActiveMatching, id_column: str
) -> List[Dict]:
    # TODO: clean this function, stop defining variables in the loop etc etc ...
    finished = False
    use_previous = False

    unlabeled: list[RecordDictPair] = []
    labeled: list[LabeledPair] = []

    n_match = len(deduper.training_pairs["match"])
    n_distinct = len(deduper.training_pairs["distinct"])

    invalid_records = dict()
    buffer_len = 1

    while not finished:
        if use_previous:
            record_pair, label = labeled.pop(0)
            if label == "match":
                n_match -= 1
            elif label == "distinct":
                n_distinct -= 1
            use_previous = False
        else:
            if not unlabeled:
                _mark_pairs(deduper, labeled)
                unlabeled = deduper.uncertain_pairs()
            try:
                record_pair = unlabeled.pop()
            except IndexError:
                break
        if any(r[id_column] in invalid_records for r in record_pair):
            continue
        _print("\n" * 3 + f"{'#' * 10}")
        for record in record_pair:
            for k, v in record.items():
                line = "%s : %s" % (k, v)
                _print(line)
            _print()
        _print(f"{n_match}/10 positive, {n_distinct}/10 negative")
        _print("Do these records refer to the same thing?")

        valid_response = False
        user_input = ""
        while not valid_response:
            if labeled:
                _print(_WITH_PREVIOUS_PROMPT)
                valid_responses = {"y", "n", "u", "l", "r", "b", "f", "p"}
            else:
                _print(_BASE_PROMPT)
                valid_responses = {"y", "n", "u", "l", "r", "b", "f"}
            user_input = input()
            if user_input in valid_responses:
                valid_response = True

        if user_input == "y":
            labeled.insert(0, (record_pair, "match"))
            n_match += 1
        elif user_input == "n":
            labeled.insert(0, (record_pair, "distinct"))
            n_distinct += 1
        elif user_input == "u":
            labeled.insert(0, (record_pair, "unsure"))
        elif user_input == "f":
            _print("Finished labeling")
            finished = True
        elif user_input == "p":
            use_previous = True
            unlabeled.append(record_pair)
        elif user_input == "l":
            invalid = record_pair[0]
            invalid_records[invalid[id_column]] = invalid
        elif user_input == "r":
            invalid = record_pair[1]
            invalid_records[invalid[id_column]] = invalid
        elif user_input == "b":
            invalid_l = record_pair[0]
            invalid_records[invalid_l[id_column]] = invalid_l
            invalid_r = record_pair[1]
            invalid_records[invalid_r[id_column]] = invalid_r

        while len(labeled) > buffer_len:
            _mark_pairs(deduper, [labeled.pop()])

    _mark_pairs(deduper, labeled)
    return list(invalid_records.values())
