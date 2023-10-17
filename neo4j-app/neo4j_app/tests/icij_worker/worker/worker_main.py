import asyncio
import multiprocessing
import sys
import tempfile
from contextlib import contextmanager
from json import JSONDecodeError
from pathlib import Path

from neo4j_app.core import AppConfig
from neo4j_app.icij_worker import Neo4jAsyncWorker
from neo4j_app.tests.icij_worker.conftest import MockWorker

_FMT = "[%(levelname)s][%(asctime)s.%(msecs)03d][%(name)s]: %(message)s"
_DATE_FMT = "%H:%M:%S"


@contextmanager
def db_path_cm(test: bool):
    if not test:
        yield None
    else:
        with tempfile.NamedTemporaryFile(prefix="db") as db_f:
            yield Path(db_f.name)


async def main():
    # Setup logger main logger
    config_path = Path(sys.argv[1])
    worker_id = sys.argv[2]
    try:
        config = AppConfig.parse_file(config_path)
    except JSONDecodeError:
        with config_path.open() as f:
            config = AppConfig.from_java_properties(f)
    with multiprocessing.Manager() as m:
        with db_path_cm(config.test) as db_path:
            if db_path is not None:
                # TODO: this will erase the DB each time, it should be done outside in
                #  case of multiple workers
                MockWorker.fresh_db(db_path)
                lock = m.Lock()
                await MockWorker.work_forever_from_config_async(
                    config, worker_id, db_path=db_path, lock=lock
                )
            else:
                await Neo4jAsyncWorker.work_forever_from_config_async(config, worker_id)


if __name__ == "__main__":
    asyncio.run(main())
