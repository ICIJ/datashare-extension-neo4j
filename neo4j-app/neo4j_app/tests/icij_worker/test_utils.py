import pytest
from pika.exceptions import StreamLostError

from neo4j_app.icij_worker.exceptions import ConnectionLostError
from neo4j_app.icij_worker.utils import parse_stream_lost_error


class _NotParsable(Exception):
    def __repr__(self) -> str:
        return f"_NotParsable__({self.args})"


@pytest.mark.parametrize(
    "stream_lost_error,expected_error",
    [
        (
            StreamLostError("Transport indicated EOF"),
            ConnectionLostError("Transport indicated EOF"),
        ),
        (
            StreamLostError(
                f"Stream connection lost: {ValueError('wrapped error')!r}"
            ),
            ValueError("wrapped error"),
        ),
        (
            StreamLostError(
                f"Stream connection lost: {_NotParsable('can''t be parsed')!r}"
            ),
            StreamLostError(
                f"Stream connection lost: {_NotParsable('can''t be parsed')!r}"
            ),
        ),
    ],
)
def test_parse_stream_lost_error(
    stream_lost_error: StreamLostError, expected_error: Exception
):
    # When
    error = parse_stream_lost_error(stream_lost_error, namespace=globals())
    # Then
    assert isinstance(error, type(expected_error))
    assert error.args == expected_error.args


def test_parse_stream_lost_error_should_raise_for_invalid_stream_lost_error():
    # Given
    stream_lost_error = StreamLostError("I'm invalid")
    # When/Then
    with pytest.raises(ValueError) as exc:
        parse_stream_lost_error(stream_lost_error, namespace=globals())
        assert exc.match("pika version is supposed to be fixed at 1.3.2")
