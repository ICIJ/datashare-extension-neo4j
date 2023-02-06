from starlette.testclient import TestClient


def test_ping(test_client: TestClient):
    # Given
    url = "ping"

    # When
    res = test_client.get(url)

    # Then
    assert res.status_code == 200, res.json()
