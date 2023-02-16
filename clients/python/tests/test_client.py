from unittest.mock import patch

import pytest

import kaskada.client


@pytest.fixture(autouse=True)
def run_around_tests():
    kaskada.client.KASKADA_DEFAULT_CLIENT = None
    kaskada.client.KASKADA_DEFAULT_SLICE = None
    yield


def test_client_init_sets_global_client():
    kaskada.client.init()
    assert kaskada.client.KASKADA_DEFAULT_CLIENT.client_id is None
    assert kaskada.client.KASKADA_DEFAULT_CLIENT.get_metadata() == []


def test_client_init_with_client_id():
    client_id = "client_id"
    kaskada.client.init(client_id=client_id)
    assert kaskada.client.KASKADA_DEFAULT_CLIENT.client_id == client_id
    assert kaskada.client.KASKADA_DEFAULT_CLIENT.get_metadata() == [
        ("client-id", "client_id")
    ]


def test_get_client_with_no_client_raises_error():
    with pytest.raises(ValueError):
        kaskada.client.get_client()

    with pytest.raises(ValueError):
        kaskada.client.get_client(None)


@patch("kaskada.client.Client")
def test_get_client_with_client_validates_client(mockClient):
    with pytest.raises(ValueError):
        mockClient.view_stub = None
        kaskada.client.get_client(mockClient)


@patch("kaskada.client.Client")
def test_get_client_with_client_returns_client(mockClient):
    result = kaskada.client.get_client(mockClient)
    assert result == mockClient


@patch("kaskada.client.Client")
def test_get_client_with_invalid_view_stub_raises_error(mockClient):
    with pytest.raises(ValueError):
        mockClient.view_stub = None
        kaskada.client.validate_client(mockClient)


@patch("kaskada.client.Client")
def test_get_client_with_invalid_table_stub_raises_error(mockClient):
    with pytest.raises(ValueError):
        mockClient.table_stub = None
        kaskada.client.validate_client(mockClient)


@patch("kaskada.client.Client")
def test_get_client_with_invalid_query_stub_raises_error(mockClient):
    with pytest.raises(ValueError):
        mockClient.table_stub = None
        kaskada.client.validate_client(mockClient)
