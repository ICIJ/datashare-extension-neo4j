import asyncio
import logging
from signal import Signals

import pytest

from icij_worker import Worker


@pytest.mark.parametrize("signal", [Signals.SIGINT, Signals.SIGTERM])
def test_worker_signal_handler(mock_worker: Worker, signal: Signals, caplog):
    # pylint: disable=protected-access
    # Given
    caplog.set_level(logging.INFO)
    worker = mock_worker
    loop = asyncio.get_event_loop()
    worker._work_forever_task = loop.create_task(mock_worker._work_forever())

    assert len(caplog.records) == 0

    # When
    worker._signal_handler(signal, graceful=True)

    # Then
    expected = [f"received {str(signal)}", "cancelling worker loop"]
    logged = caplog.messages
    for ex in expected:
        assert any(l.startswith(ex) for l in logged)
