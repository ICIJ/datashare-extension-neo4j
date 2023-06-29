import logging
import sys

from pika.exchange_type import ExchangeType

from neo4j_app import icij_worker
from neo4j_app.icij_worker import Exchange, MessageConsumer, Routing

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
    task_routing = Routing(
        exchange=Exchange(name="default-ex", type=ExchangeType.topic),
        default_queue="test-queue",
        routing_key="test",
    )
    consumer = MessageConsumer(
        name="test-worker",
        on_message=lambda: print("working"),
        broker_url=broker_url,
        task_routing=task_routing,
    )
    consumer.consume()
