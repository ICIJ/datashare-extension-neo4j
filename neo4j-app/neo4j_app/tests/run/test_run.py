import subprocess
from pathlib import Path

import pytest

from neo4j_app import ROOT_DIR


def test_should_read_java_properties(tmpdir: Path):
    # Given
    missing_config_file_path = tmpdir / "missing.properties"
    run_path = ROOT_DIR.joinpath("run", "run.py")

    # When
    with pytest.raises(subprocess.CalledProcessError) as exception_info:
        cmd = [
            "python",
            str(run_path),
            "--config-path",
            str(missing_config_file_path),
            "--force-migrations",
        ]
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            encoding="utf-8",
        )

    exception = exception_info.value

    # Then
    assert exception.returncode == 1
    assert "Provided config path does not exists" in exception.stderr
    # Check that the logger was used
    assert "ERROR" in exception.stderr
    assert str(missing_config_file_path) in exception.stderr
