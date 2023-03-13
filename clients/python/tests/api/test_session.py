import pytest

import kaskada.api.session as session


def test_builder_defaults():
    builder = session.Builder()
    assert builder._endpoint is None
    assert builder._is_secure is None
    assert builder._client_id is None
    assert builder._name is not None


def test_builder_set_endpoint_sets_endpoint_and_is_secure():
    endpoint = "my_endpoint"
    is_secure = True
    builder = session.Builder().endpoint(endpoint, is_secure)
    assert builder._endpoint == endpoint
    assert builder._is_secure == is_secure
    assert builder._client_id is None
    assert builder._name is not None


def test_builder_set_name_sets_name():
    name = "my_name"
    builder = session.Builder().name(name)
    assert builder._name == name


def test_builder_set_client_id_sets_client_id():
    client_id = "my_client_id"
    builder = session.Builder().client_id(client_id)
    assert builder._client_id == client_id


def test_local_builder_defaults():
    builder = session.LocalBuilder()
    assert builder._endpoint == session.KASKADA_ENDPOINT_DEFAULT
    assert builder._is_secure == session.KASKADA_IS_SECURE_DEFAULT
    assert builder._client_id is None
    assert builder._name is not None
    assert builder._path == session.LocalBuilder.KASKADA_PATH_DEFAULT
    assert builder._bin_path == session.LocalBuilder.KASKADA_BIN_PATH_DEFAULT
    assert builder._log_path == session.LocalBuilder.KASKADA_LOG_PATH_DEFAULT
    assert builder._download == True
    assert builder._manager_configs == {}


def test_local_builder_set_path_sets_path():
    path = "my_path"
    builder = session.LocalBuilder().path(path)
    assert builder._path == path


def test_local_builder_set_log_dir_sets_log_dir():
    log_dir = "my_log_dir"
    builder = session.LocalBuilder().log_path(log_dir)
    assert builder._log_path == log_dir


def test_local_builder_no_path_throws_exception_build():
    path = None
    with pytest.raises(ValueError):
        builder = session.LocalBuilder().path(path).download(False).build()


def test_local_builder_no_log_path_throws_exception_build():
    path = None
    with pytest.raises(ValueError):
        builder = session.LocalBuilder().log_path(path).download(False).build()


def test_local_builder_no_bin_path_throws_exception_build():
    path = None
    with pytest.raises(ValueError):
        builder = session.LocalBuilder().bin_path(path).download(False).build()


def test_local_builder_set_manager_rest_port_sets_config():
    port = 12345
    builder = session.LocalBuilder().manager_rest_port(port=port)
    assert builder._manager_configs["-rest-port"] == port


def test_local_builder_set_manager_port_sets_config():
    port = 12345
    builder = session.LocalBuilder().manager_grpc_port(port=port)
    assert builder._manager_configs["-grpc-port"] == port
