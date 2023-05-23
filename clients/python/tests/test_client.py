from unittest.mock import patch

import pytest

import kaskada.client
import kaskada.slice_filters

endpoint = "some_endpoint_locally"
is_secure = True
polling_interval_seconds = 0.12345
client_id = "some-awkard-taco"


def test_client_factory_returns_set_params():
    client_factory = kaskada.client.ClientFactory(
        client_id=client_id,
        endpoint=endpoint,
        is_secure=is_secure,
        polling_interval_seconds=polling_interval_seconds,
    )
    result = client_factory.get_client(should_connect=False)
    assert result.endpoint == endpoint
    assert result.client_id == client_id
    assert result.is_secure == is_secure
    assert result.polling_interval_seconds == polling_interval_seconds
    assert result.is_ready() == False


def test_client_sets_params():
    result = kaskada.client.Client(
        client_id=client_id,
        endpoint=endpoint,
        is_secure=is_secure,
        polling_interval_seconds=polling_interval_seconds,
        should_connect=False,
    )
    assert result.endpoint == endpoint
    assert result.client_id == client_id
    assert result.is_secure == is_secure
    assert result.polling_interval_seconds == polling_interval_seconds
    assert result.is_ready() == False


@pytest.fixture(autouse=True)
def run_around_tests():
    kaskada.client.reset()
    yield


def test_client_get_metadata():
    result = kaskada.client.Client(
        client_id=client_id,
        endpoint=endpoint,
        is_secure=is_secure,
        polling_interval_seconds=polling_interval_seconds,
        should_connect=False,
    )
    assert result.get_metadata() == [("client-id", client_id)]


def test_client_no_client_id_empty_metadata():
    result = kaskada.client.Client(
        client_id=None,
        endpoint=endpoint,
        is_secure=is_secure,
        polling_interval_seconds=polling_interval_seconds,
        should_connect=False,
    )
    assert result.get_metadata() == []


def test_set_default_slice():
    expected = kaskada.slice_filters.EntityFilter(["some_entity"])
    kaskada.client.set_default_slice(expected)
    assert kaskada.client.KASKADA_DEFAULT_SLICE == expected


def test_set_default_client():
    expected = kaskada.client.Client(
        client_id=None,
        endpoint=endpoint,
        is_secure=is_secure,
        polling_interval_seconds=polling_interval_seconds,
        should_connect=False,
    )
    kaskada.client.set_default_client(expected)
    assert kaskada.client.KASKADA_DEFAULT_CLIENT == expected


def test_reset():
    kaskada.client.set_default_slice(
        kaskada.slice_filters.EntityFilter(["some_entity"])
    )
    kaskada.client.set_default_client(
        kaskada.client.Client(
            client_id=None,
            endpoint=endpoint,
            is_secure=is_secure,
            polling_interval_seconds=polling_interval_seconds,
            should_connect=False,
        )
    )
    kaskada.client.reset()
    assert kaskada.client.KASKADA_DEFAULT_CLIENT == None
    assert kaskada.client.KASKADA_DEFAULT_SLICE == None


def test_get_client():
    # Default case: no client initialized and no client provided
    with pytest.raises(ValueError):
        kaskada.client.get_client()

    # case: provided client is not ready
    local_client = kaskada.client.Client(
        client_id=None,
        endpoint=endpoint,
        is_secure=is_secure,
        polling_interval_seconds=polling_interval_seconds,
        should_connect=False,
    )
    result = kaskada.client.get_client(local_client, validate=False)
    assert result == local_client

    # case: global client is set
    kaskada.client.set_default_client(local_client)
    result = kaskada.client.get_client(validate=False)
    assert result == local_client

    # case: global client is set but new client is provided
    expected = kaskada.client.Client(
        client_id=None,
        endpoint="another-random-endpoint",
        is_secure=is_secure,
        polling_interval_seconds=polling_interval_seconds,
        should_connect=False,
    )
    result = kaskada.client.get_client(expected, validate=False)
    assert result == expected
