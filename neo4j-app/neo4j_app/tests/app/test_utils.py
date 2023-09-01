from starlette.testclient import TestClient


def test_request_validation_error_handler(error_test_client_session: TestClient):
    # Given
    client = error_test_client_session
    url = "/internal-errors"

    # When
    res = client.post(url, json=dict())

    # Then
    assert res.status_code == 400
    error = res.json()
    assert error["title"] == "Request Validation Error"
    expected_detail = """body -> mandatory_field
  field required (type=value_error.missing)"""
    assert error["detail"] == expected_detail


def test_http_exception_error_handler(error_test_client_session: TestClient):
    # Given
    # client = error_test_client_session
    client = error_test_client_session
    url = "/idontexist"

    # When
    res = client.get(url)

    # Then
    assert res.status_code == 404
    error = res.json()
    assert error["title"] == "Not Found"
    assert error["detail"] == "Not Found"


def test_internal_error_handler(error_test_client_session: TestClient):
    # Given
    client = error_test_client_session
    url = "/internal-errors/generate"

    # When
    res = client.get(url)

    # Then
    assert res.status_code == 500
    error = res.json()
    assert error["title"] == "Internal Server Error"
    assert error["detail"] == "ValueError: this is the internal error"
    assert "this is the internal error" in error["trace"]
