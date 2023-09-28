import logging
import multiprocessing
import sys
import tempfile
from pathlib import Path

from neo4j_app import icij_worker
from neo4j_app.core import AppConfig
from neo4j_app.tests.icij_worker.conftest import MockWorker

_FMT = "[%(levelname)s][%(asctime)s.%(msecs)03d][%(name)s]: %(message)s"
_DATE_FMT = "%H:%M:%S"


if __name__ == "__main__":
    # Setup logger main logger
    config_path = sys.argv[1]
    worker_id = sys.argv[2]
    loggers = ["__main__", icij_worker.__name__]
    for logger in loggers:
        logger = logging.getLogger(logger)
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
        logger.addHandler(handler)
    with multiprocessing.Manager() as m:
        with tempfile.NamedTemporaryFile(prefix="db") as db_f:
            config_path = AppConfig.parse_file(config_path)
            queue = m.Queue()
            lock = m.Lock()
            db_path = Path(db_f.name)
            MockWorker.work_forever_from_config(
                config_path, worker_id, queue=queue, db_path=db_path, lock=lock
            )
