from itertools import islice
from typing import Iterable


def batch(iterable: Iterable, batch_size: int):
    if batch_size < 1:
        raise ValueError("batch_size must be at least one")
    it = iter(iterable)
    while it_batch := tuple(islice(it, batch_size)):
        yield it_batch
