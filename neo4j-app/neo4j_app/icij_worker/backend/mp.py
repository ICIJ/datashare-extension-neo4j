import functools
import logging
import multiprocessing
import os
import signal
import sys
from contextlib import contextmanager
from typing import Dict, Optional

from neo4j_app.icij_worker import AsyncApp, Worker, WorkerConfig

logger = logging.getLogger(__name__)

_HANDLED_SIGNALS = [signal.SIGTERM, signal.SIGINT]
if sys.platform == "win32":
    _HANDLED_SIGNALS += [signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT]


def _mp_work_forever(
    app: str,
    config: WorkerConfig,
    worker_id: str,
    *,
    worker_extras: Optional[Dict] = None,
    app_deps_extras: Optional[Dict] = None,
):
    if worker_extras is None:
        worker_extras = dict()
    if app_deps_extras is None:
        app_deps_extras = dict()
    # For multiprocessing, lifespan dependencies need to be run once per process
    app = AsyncApp.load(app)
    deps_cm = app.lifetime_dependencies(worker_id=worker_id, **app_deps_extras)
    worker = Worker.from_config(config, app=app, worker_id=worker_id, **worker_extras)
    # This is ugly, but we have to work around the fact that we can't use asyncio code
    # here
    worker.loop.run_until_complete(
        deps_cm.__aenter__()  # pylint: disable=unnecessary-dunder-call
    )
    try:
        worker.work_forever()
    finally:
        worker.info("worker stopped working, tearing down %s dependencies", app.name)
        worker.loop.run_until_complete(deps_cm.__aexit__(*sys.exc_info()))


def signal_handler(sig_num, *_, pool: multiprocessing.Pool):
    logger.error(
        "received %s, triggering process pool worker shutdown !",
        signal.Signals(sig_num).name,
    )


def setup_main_process_signal_handlers(pool: multiprocessing.Pool):
    handler = functools.partial(signal_handler, pool=pool)
    for s in _HANDLED_SIGNALS:
        signal.signal(s, handler)


@contextmanager
def run_workers_with_multiprocessing(
    app: str,
    n_workers: int,
    config: WorkerConfig,
    *,
    handle_signals: bool = True,
    worker_extras: Optional[Dict] = None,
    app_deps_extras: Optional[Dict] = None,
):
    logger.info("Creating multiprocessing worker pool with %s workers", n_workers)
    # Here we set maxtasksperchild to 1. Each worker has a single never ending task
    # which consists in working forever. Additionally, in some cases using
    # maxtasksperchild=1 seems to help to terminate the worker pull
    # (cpython bug: https://github.com/python/cpython/pull/8009)
    mp_ctx = multiprocessing.get_context("spawn")
    main_process_id = os.getpid()
    # TODO: make this a bit more informative be for instance adding the child process ID
    worker_ids = [f"worker-{main_process_id}-{i}" for i in range(n_workers)]
    kwds = {"app": app, "config": config}
    kwds["worker_extras"] = worker_extras
    kwds["app_deps_extras"] = app_deps_extras
    pool = mp_ctx.Pool(n_workers, maxtasksperchild=1)
    logger.debug("Setting up signal handlers...")
    if handle_signals:
        setup_main_process_signal_handlers(pool)
    try:
        for w_id in worker_ids:
            kwds.update({"worker_id": w_id})
            logger.info("starting worker %s", w_id)
            pool.apply_async(_mp_work_forever, kwds=kwds)
        yield
    except KeyboardInterrupt as e:
        if not handle_signals:
            logger.info(
                "received %s, triggering process pool worker shutdown !",
                KeyboardInterrupt.__name__,
            )
        else:
            msg = (
                f"Received {KeyboardInterrupt.__name__} while SIGINT was expected to"
                f" be handled"
            )
            raise RuntimeError(msg) from e
    finally:
        logger.info("Sending termination signal to workers (SIGTERM)...")
        pool.terminate()
        pool.join()  # Wait forever
        logger.info("Terminated worker pool !")
