# We exclude this one from the main test folder to avoid running it every time since
# it can be very long
import os
from pathlib import Path
from typing import Generator

from pylint.lint import Run

_NO_ERROR_STATUS = 0

_ROOT_PATH = Path(__file__).parent
_RCFILEPATH = _ROOT_PATH.parent.joinpath("qa", "python", "pylintrc")
_ICIJ_COMMON = "icij_common"


def _files_with_ext(path: Path, ext: str) -> Generator[Path, None, None]:
    for dir, _, filenames in os.walk(path):
        for f in filenames:
            if f.endswith(ext):
                yield Path(dir) / f


def _assert_pylint_linting(path: Path):
    # TODO: use pyproject.toml instead
    args = [
        "--rcfile",
        str(_RCFILEPATH),
        "--load-plugins=pylint.extensions.bad_builtin",
    ]
    args += [str(p) for p in _files_with_ext(path, ".py")]
    run = Run(args, exit=False)
    assert run.linter.msg_status == _NO_ERROR_STATUS


def test_python_files_linting():
    _assert_pylint_linting(_ROOT_PATH / _ICIJ_COMMON)
