from unittest.mock import MagicMock, call, patch

import pytest

import kaskada.api.session
import kaskada.client

endpoint = "some_endpoint"
is_secure = True


@pytest.fixture(autouse=True)
def run_around_tests():
    kaskada.client.reset()
    yield


@patch("kaskada.client.ClientFactory")
@patch("kaskada.client.Client")
def test_session(client_factory, client):
    session = kaskada.api.session.Session(
        endpoint=endpoint, is_secure=is_secure, client_factory=client_factory
    )
    assert session._endpoint == endpoint
    assert session._is_secure == is_secure
    assert session._client_id is None
    assert session.client_factory == client_factory

    client_factory.get_client = MagicMock(return_value=client)
    result = session.connect()
    assert result == client
    assert kaskada.client.KASKADA_DEFAULT_CLIENT == client

    session.stop()
    assert kaskada.api.session.KASKADA_SESSION is None
    assert kaskada.client.KASKADA_DEFAULT_CLIENT is None


def test_builder_defaults():
    builder = kaskada.api.session.Builder()
    assert builder._endpoint is None
    assert builder._is_secure is None
    assert builder._client_id is None
    assert builder._name is None


def test_builder_set_endpoint_sets_endpoint_and_is_secure():
    endpoint = "my_endpoint"
    is_secure = True
    builder = kaskada.api.session.Builder().endpoint(endpoint, is_secure)
    assert builder._endpoint == endpoint
    assert builder._is_secure == is_secure
    assert builder._client_id is None


def test_builder_set_name_sets_name():
    name = "my_name"
    builder = kaskada.api.session.Builder().name(name)
    assert builder._name == name


def test_builder_set_client_id_sets_client_id():
    client_id = "my_client_id"
    builder = kaskada.api.session.Builder().client_id(client_id)
    assert builder._client_id == client_id


def test_local_builder_defaults():
    builder = kaskada.api.session.LocalBuilder()
    assert builder._endpoint == kaskada.client.KASKADA_DEFAULT_ENDPOINT
    assert builder._is_secure == kaskada.client.KASKADA_IS_SECURE
    assert builder._client_id is None
    assert builder._path == kaskada.api.session.KASKADA_PATH_DEFAULT
    assert builder._bin_path == kaskada.api.session.KASKADA_BIN_PATH_DEFAULT
    assert builder._log_path == kaskada.api.session.KASKADA_LOG_PATH_DEFAULT
    assert builder._download == True
    assert builder.manager_configs == {"-db-in-memory": "0", "-no-color": "1"}
    assert builder.engine_configs == {"--log-no-color": "1"}


def test_local_builder_set_engine_version():
    version = "engine@v0.0.1-beta.1"
    builder = kaskada.api.session.LocalBuilder().engine_version(version)
    assert builder._engine_version == version


def test_local_builder_set_engine_version_throws():
    invalid_versions = ["0.0.1", "engine@0.0.1", "engine@v23", "manager@v1.1.1"]

    for version in invalid_versions:
        with pytest.raises(ValueError):
            builder = kaskada.api.session.LocalBuilder().engine_version(version)


def test_local_builder_set_path_sets_path():
    path = "my_path"
    builder = kaskada.api.session.LocalBuilder().path(path)
    assert builder._path == path


def test_local_builder_set_log_dir_sets_log_dir():
    log_dir = "my_log_dir"
    builder = kaskada.api.session.LocalBuilder().log_path(log_dir)
    assert builder._log_path == log_dir


def test_local_builder_no_path_throws_exception_build():
    path = None
    with pytest.raises(ValueError):
        builder = kaskada.api.session.LocalBuilder().path(path).download(False).build()


def test_local_builder_no_log_path_throws_exception_build():
    path = None
    with pytest.raises(ValueError):
        builder = (
            kaskada.api.session.LocalBuilder().log_path(path).download(False).build()
        )


def test_local_builder_no_bin_path_throws_exception_build():
    path = None
    with pytest.raises(ValueError):
        builder = (
            kaskada.api.session.LocalBuilder().bin_path(path).download(False).build()
        )


def test_local_builder_set_manager_rest_port_sets_config():
    port = 12345
    builder = kaskada.api.session.LocalBuilder().manager_rest_port(port=port)
    assert builder.manager_configs["-rest-port"] == port


def test_local_builder_set_manager_port_sets_config():
    port = 12345
    builder = kaskada.api.session.LocalBuilder().manager_grpc_port(port=port)
    assert builder.manager_configs["-grpc-port"] == port
