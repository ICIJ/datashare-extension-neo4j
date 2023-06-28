import logging
import sys

from neo4j_app import icij_worker
from neo4j_app.icij_worker import MessageConsumer

_FMT = "[%(levelname)s][%(asctime)s.%(msecs)03d][%(name)s]: %(message)s"
_DATE_FMT = "%H:%M:%S"


if __name__ == "__main__":
    # Setup logger main logger
    broker_url = sys.argv[1]
    loggers = ["__main__", icij_worker.__name__]
    for logger in loggers:
        logger = logging.getLogger(logger)
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
        logger.addHandler(handler)
    consumer = MessageConsumer(
        name="test-worker",
        on_message=lambda: print("working"),
        broker_url=broker_url,
        queue="test-queue",
        exchange="default-ex",
        routing_key="test",
    )
    consumer.consume()
