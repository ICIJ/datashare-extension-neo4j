import logging
import sys

import proton
import python_worker
import stomp
from python_worker.icij_worker import main_amqp as icij_worker_main_amqp

_STREAM_HANDLER_FMT = "[%(levelname)s][%(asctime)s.%(msecs)03d][%(name)s]: %(message)s"
_DATE_FMT = "%H:%M:%S"


def _setup_loggers():
    level = logging.DEBUG
    loggers = [
        "__main__",
        python_worker.__name__,
        "icij_worker",
        stomp.__name__,
        proton.__name__,
    ]
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(logging.Formatter(_STREAM_HANDLER_FMT, _DATE_FMT))
    stream_handler.setLevel(level)

    for logger in loggers:
        logger = logging.getLogger(logger)
        logger.setLevel(level)
        logger.handlers = []
        logger.addHandler(stream_handler)


if __name__ == "__main__":
    from python_worker.tasks import app

    _setup_loggers()
    # icij_worker_main(app)
    icij_worker_main_amqp(app)
