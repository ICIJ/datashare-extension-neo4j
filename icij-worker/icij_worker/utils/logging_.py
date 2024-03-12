import logging
import sys
from typing import List, Optional

from icij_common.logging_utils import (
    DATE_FMT,
    STREAM_HANDLER_FMT,
    STREAM_HANDLER_FMT_WITH_WORKER_ID,
    WorkerIdFilter,
)
from icij_common.pydantic_utils import get_field_default_value
from pydantic.fields import FieldInfo


class LogWithWorkerIDMixin:
    def setup_loggers(self, worker_id: Optional[str] = None):
        # Ugly work around the Pydantic V1 limitations...
        loggers = self.loggers
        if isinstance(loggers, FieldInfo):
            loggers = get_field_default_value(loggers)
        log_level = self.log_level
        if isinstance(log_level, FieldInfo):
            log_level = get_field_default_value(log_level)
        force_warning = getattr(self, "force_warning_loggers", [])
        if isinstance(force_warning, FieldInfo):
            force_warning = get_field_default_value(force_warning)
        force_warning = set(force_warning)
        worker_id_filter = None
        if worker_id is not None:
            worker_id_filter = WorkerIdFilter(worker_id)
        handlers = self._handlers(worker_id_filter, log_level)
        for logger in loggers:
            logger = logging.getLogger(logger)
            level = getattr(logging, log_level)
            if logger.name in force_warning:
                level = max(logging.WARNING, level)
            logger.setLevel(level)
            logger.handlers = []
            for handler in handlers:
                logger.addHandler(handler)

    def _handlers(
        self, worker_id_filter: Optional[logging.Filter], log_level: int
    ) -> List[logging.Handler]:
        stream_handler = logging.StreamHandler(sys.stderr)
        if worker_id_filter is not None:
            fmt = STREAM_HANDLER_FMT_WITH_WORKER_ID
        else:
            fmt = STREAM_HANDLER_FMT
        log_in_json = getattr(self, "log_in_json", False)
        if isinstance(log_in_json, FieldInfo):
            log_in_json = get_field_default_value(log_in_json)
        if log_in_json:
            # TO be installed and required on child libs
            # TODO: add it as an extra of this lib
            from pythonjsonlogger.jsonlogger import JsonFormatter

            fmt = JsonFormatter(fmt, DATE_FMT)
        else:
            fmt = logging.Formatter(fmt, DATE_FMT)
        stream_handler.setFormatter(fmt)
        handlers = [stream_handler]
        for handler in handlers:
            if worker_id_filter is not None:
                handler.addFilter(worker_id_filter)
            handler.setLevel(log_level)
        return handlers
