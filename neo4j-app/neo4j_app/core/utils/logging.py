import contextlib
import logging
from datetime import datetime
from functools import wraps
from typing import Optional


class DifferedLoggingMessage:
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return str(self.fn(*self.args, **self.kwargs))


def log_elapsed_time(
    logger: logging.Logger, level: int, output_msg: Optional[str] = None
):
    if output_msg is None:
        output_msg = "Elapsed time ->:\n{elapsed_time}"

    def get_wrapper(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            start = datetime.now()
            msg_fmt = dict()
            res = fn(*args, **kwargs)
            if "elapsed_time" in output_msg:
                msg_fmt["elapsed_time"] = datetime.now() - start
            logger.log(level, output_msg.format(**msg_fmt))
            return res

        return wrapped

    return get_wrapper


@contextlib.contextmanager
def log_elapsed_time_cm(
    logger: logging.Logger, level: int, output_msg: Optional[str] = None
):
    if output_msg is None:
        output_msg = "Elapsed time ->:\n{elapsed_time}"
    start = datetime.now()
    yield
    end = datetime.now() - start
    msg_fmt = dict()
    if "elapsed_time" in output_msg:
        msg_fmt["elapsed_time"] = end
    logger.log(level, output_msg.format(**msg_fmt))
