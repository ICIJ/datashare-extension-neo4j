from concurrent.futures import Executor
from contextlib import contextmanager


@contextmanager
def shutdown_nowait(executor: Executor):
    try:
        yield executor
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
