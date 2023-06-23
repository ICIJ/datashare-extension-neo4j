import logging
from typing import Mapping, Type

from pika.exceptions import StreamLostError

from neo4j_app.icij_worker.exceptions import ConnectionLostError


class LogWithNameMixin:
    _logger: logging.Logger
    logged_name: str

    def _log(self, level: int, msg: str, *args, **kwargs):
        msg = f"{self.logged_name}: {msg}"
        self._logger.log(level, msg, *args, **kwargs)


_EOF_MSG = "Transport indicated EOF"
_OTHER_ERROR_MSG = "Stream connection lost: "
_PIKA_VERSION_ERROR_MSG = "pika version is supposed to be fixed at 1.3.2, this is not \
the case any longer, error handling should be updated accordingly"


# Ugly but necessary for now, see https://groups.google.com/g/pika-python/c/G4rzLB7s5E0
def parse_stream_lost_error(
    error: StreamLostError, namespace: Mapping[str, Type[Exception]]
) -> Exception:
    error_msg: str = error.args[0]
    if error_msg.startswith(_EOF_MSG):
        return ConnectionLostError(error_msg)
    if error_msg.startswith(_OTHER_ERROR_MSG):
        # In general eval(repr(o)) will allow to retrieve o
        error_type = error_msg.lstrip(_OTHER_ERROR_MSG)
        if not error_type:
            return error
        try:
            return eval(error_type, namespace)  # pylint: disable=eval-used
        except (NameError, SyntaxError):
            return error
    raise ValueError(_PIKA_VERSION_ERROR_MSG)
