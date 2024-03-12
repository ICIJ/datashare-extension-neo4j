import asyncio
import contextlib
import os
import traceback
from copy import copy
from time import monotonic, sleep
from typing import Awaitable, Callable, Optional

import pytest

TEST_PROJECT = "test_project"


def true_after(
    state_statement: Callable, *, after_s: float, sleep_s: float = 0.01
) -> bool:
    start = monotonic()
    while "waiting for the statement to be True":
        try:
            assert state_statement()
            return True
        except AssertionError:
            if monotonic() - start < after_s:
                sleep(sleep_s)
                continue
            return False


async def async_true_after(
    state_statement: Callable[[], Awaitable[bool]],
    *,
    after_s: float,
    sleep_s: float = 0.01,
) -> bool:
    start = monotonic()
    while "waiting for the statement to be True":
        try:
            assert await state_statement()
            return True
        except AssertionError:
            if monotonic() - start < after_s:
                await asyncio.sleep(sleep_s)
                continue
            return False


@contextlib.contextmanager
def fail_if_exception(msg: Optional[str] = None):
    try:
        yield
    except Exception as e:  # pylint: disable=W0703
        trace = "".join(traceback.format_exception(None, e, e.__traceback__))
        if msg is None:
            msg = "Test failed due to the following error"
        pytest.fail(f"{msg}\n{trace}")


@pytest.fixture()
def reset_env():
    old_env = copy(dict(os.environ))
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_env)


# Define a session level even_loop fixture to overcome limitation explained here:
# https://github.com/tortoise/tortoise-orm/issues/638#issuecomment-830124562
@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()
