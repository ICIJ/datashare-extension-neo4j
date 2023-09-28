import signal
import sys
from pathlib import Path
from subprocess import PIPE, Popen

from neo4j_app.core import AppConfig
from neo4j_app.tests.conftest import true_after


def test_worker_should_close_gracefully_on_sigint(test_config: AppConfig, tmpdir: Path):
    # Given
    config_path = Path(tmpdir) / "config.json"
    main_test_path = Path(__file__).parent / "worker_main.py"

    # Then
    config_path.write_text(test_config.json(by_alias=True, exclude_unset=True))
    cmd = [sys.executable, main_test_path, config_path, "test-worker-id"]
    with Popen(cmd, stderr=PIPE, stdout=PIPE, text=True) as p:
        # Wait for the consumer to be running
        assert true_after(
            lambda: any("started working" in line for line in p.stderr), after_s=2.0
        ), "Failed to start worker"
        # Kill it
        p.send_signal(signal.SIGINT)
        assert true_after(
            lambda: any("shutting down gracefully" in line for line in p.stderr),
            after_s=2.0,
        ), "Failed to shutdown worker gracefully"


def test_worker_should_close_immediately_on_sigterm(
    test_config: AppConfig, tmpdir: Path
):
    # Given
    config_path = Path(tmpdir) / "config.json"
    main_test_path = Path(__file__).parent / "worker_main.py"

    # Then
    config_path.write_text(test_config.json(by_alias=True, exclude_unset=True))
    cmd = [sys.executable, main_test_path, config_path, "test-worker-id"]
    with Popen(cmd, stderr=PIPE, stdout=PIPE, text=True) as p:
        # Wait for the consumer to be running
        assert true_after(
            lambda: any("started working" in line for line in p.stderr), after_s=2.0
        ), "Failed to start worker"
        # Kill it
        p.send_signal(signal.SIGTERM)
        assert true_after(
            lambda: any("shutting down the hard way" in line for line in p.stderr),
            after_s=2.0,
        ), "Failed to shutdown consumer immediately"
