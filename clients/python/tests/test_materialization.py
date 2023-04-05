from typing import Any, Dict
from unittest.mock import patch
from uuid import UUID

import pytest

import kaskada.kaskada.v1alpha.materialization_service_pb2 as material_pb
from kaskada.materialization import (
    Destination,
    FileType,
    MaterializationView,
    ObjectStoreDestination,
    PulsarDestination,
    RedisDestination,
    create_materialization,
    delete_materialization,
    get_materialization,
    list_materializations,
    to_with_views,
)
from kaskada.slice_filters import EntityFilter


def test_redis_destination_to_request():
    params = {
        "host_name": "my_host_name",
        "port": 1234,
        "use_tls": False,
        "database_number": 4321,
        "password": "my_password",
        "tls_cert": "my_tls_cert",
        "tls_key": "my_tls_key",
        "tls_ca_cert": "my_tls_ca_cert",
        "insecure_skip_verify": True,
    }

    result = RedisDestination(**params)
    assert result.to_request() == params


def test_object_store_destination_to_request():
    csv_file = FileType.FILE_TYPE_CSV
    output_prefix = "/my_prefix"
    csv_object_store = ObjectStoreDestination(csv_file, output_prefix)
    assert csv_object_store.to_request() == {
        "file_type": "FILE_TYPE_CSV",
        "output_prefix_uri": "file://" + output_prefix,
    }

    csv_file = FileType.FILE_TYPE_CSV
    output_prefix = "file:///my_prefix"
    csv_object_store = ObjectStoreDestination(csv_file, output_prefix)
    assert csv_object_store.to_request() == {
        "file_type": "FILE_TYPE_CSV",
        "output_prefix_uri": output_prefix,
    }

    parquet_file = FileType.FILE_TYPE_PARQUET
    parquet_object_store = ObjectStoreDestination(parquet_file, output_prefix)
    assert parquet_object_store.to_request() == {
        "file_type": "FILE_TYPE_PARQUET",
        "output_prefix_uri": output_prefix,
    }

    unspecified_file = FileType.FILE_TYPE_UNSPECIFIED
    unspecified_object_store = ObjectStoreDestination(unspecified_file, output_prefix)
    assert unspecified_object_store.to_request() == {
        "file_type": "FILE_TYPE_UNSPECIFIED",
        "output_prefix_uri": output_prefix,
    }


def test_object_store_destination_fails_for_invalid_output_uri():
    csv_file = FileType.FILE_TYPE_CSV
    output_prefix = "my_prefix"
    csv_object_store = ObjectStoreDestination(csv_file, output_prefix)
    with pytest.raises(ValueError) as value_exc:
        csv_object_store.to_request()
    assert (
        'output_prefix_uri must be a file uri or absolute path. Try prefixing with "file:///"'
        in str(value_exc.value)
    )

    parquet_file = FileType.FILE_TYPE_PARQUET
    output_prefix = "file://my_prefix"
    parquet_object_store = ObjectStoreDestination(parquet_file, output_prefix)
    with pytest.raises(ValueError) as value_exc:
        parquet_object_store.to_request()
    assert (
        'output_prefix_uri must be a file uri or absolute path. Try prefixing with "file:///"'
        in str(value_exc.value)
    )


def test_pulsar_destination_to_request():
    tenant = "tenant"
    namespace = "namespace"
    topic_name = "name"
    broker_service_url = "pulsar://127.0.0.1:6650"

    pulsar = PulsarDestination(tenant, namespace, topic_name, broker_service_url)
    assert pulsar.to_request() == {
        "tenant": "tenant",
        "namespace": "namespace",
        "topic_name": "name",
        "broker_service_url": "pulsar://127.0.0.1:6650",
        "admin_service_url": "http://127.0.0.1:8080",
        "auth_params": None,
        "auth_plugin": None,
    }

    pulsar_defaults = PulsarDestination()
    request_defaults = pulsar_defaults.to_request()
    try:
        uuid_obj = UUID(request_defaults["topic_name"], version=4)
    except ValueError:
        assert False, "expected default name as uuid"

    assert request_defaults["tenant"] == "public"
    assert request_defaults["namespace"] == "default"
    assert request_defaults["broker_service_url"] == "pulsar://127.0.0.1:6650"


def test_pulsar_generates_unique_uuids():
    pulsar1 = PulsarDestination()
    topic1 = pulsar1.to_request()["topic_name"]

    pulsar2 = PulsarDestination()
    topic2 = pulsar2.to_request()["topic_name"]

    pulsar_whitespace = PulsarDestination(topic_name="   ")
    topic_whitespace = pulsar_whitespace.to_request()["topic_name"]
    assert not len(topic_whitespace.strip()) == 0

    assert topic1 != topic2 != topic_whitespace


def test_materialization_view_constructor():
    name = "my_view"
    expression = "my_expression"

    materialization_view = MaterializationView(name, expression)
    assert materialization_view._name == name
    assert materialization_view._expression == expression


@patch("kaskada.client.Client")
def test_delete_materialization_calls_stub(mockClient):
    name = "my_materialization"
    expected_request = material_pb.DeleteMaterializationRequest(
        materialization_name=name
    )

    delete_materialization(name, client=mockClient)
    mockClient.materialization_stub.DeleteMaterialization.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_get_materialization_calls_stub(mockClient):
    name = "my_materialization"
    expected_request = material_pb.GetMaterializationRequest(materialization_name=name)

    get_materialization(name, client=mockClient)
    mockClient.materialization_stub.GetMaterialization.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_list_materialization_calls_stub(mockClient):
    expected_request = material_pb.ListMaterializationsRequest(search=None)

    list_materializations(client=mockClient)
    mockClient.materialization_stub.ListMaterializations.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )

    search = "tacos"
    expected_request = material_pb.ListMaterializationsRequest(search=search)

    list_materializations(search=search, client=mockClient)
    mockClient.materialization_stub.ListMaterializations.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


def test_create_materialization_raises_exception_invalid_destination():
    class TestDestination(Destination):
        def to_request(self) -> Dict[str, Any]:
            return {}

    name = "my_materialization"
    query = "my_query"
    destination = TestDestination()  # Abstract class should not be used
    views = []
    with pytest.raises(Exception):
        create_materialization(name, query, destination, views)


"""
materialization_name: "my_awkward_tacos"
  query: "last(tacos)"
  with_views {
    name: "my_second_view"
    expression: "last(awkward)"
  }
  destination {
    object_store {
      file_type: FILE_TYPE_CSV
      output_prefix_uri: "prefix"
    }
  }
  slice {
    entity_keys {
      entity_keys: "my_entity_a"
      entity_keys: "my_entity_b"
    }
  }
  
"""


@patch("kaskada.client.Client")
def test_create_materialization_object_store_destination(mockClient):
    name = "my_awkward_tacos"
    expression = "last(tacos)"
    destination = ObjectStoreDestination(FileType.FILE_TYPE_CSV, "file:///prefix")
    views = [MaterializationView("my_second_view", "last(awkward)")]
    slice_filter = EntityFilter(["my_entity_a", "my_entity_b"])

    expected_request = material_pb.CreateMaterializationRequest(
        **{
            "materialization": {
                "materialization_name": name,
                "expression": expression,
                "with_views": [
                    {"name": "my_second_view", "expression": "last(awkward)"}
                ],
                "destination": {
                    "object_store": {
                        "file_type": "FILE_TYPE_CSV",
                        "output_prefix_uri": "file:///prefix",
                    }
                },
                "slice": slice_filter.to_request(),
            }
        }
    )
    create_materialization(
        name,
        expression,
        destination,
        views,
        slice_filter=slice_filter,
        client=mockClient,
    )
    mockClient.materialization_stub.CreateMaterialization.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_create_materialization_object_store_parquet_destination(mockClient):
    name = "my_awkward_tacos"
    expression = "last(tacos)"
    destination = ObjectStoreDestination(FileType.FILE_TYPE_PARQUET, "/prefix")
    views = [MaterializationView("my_second_view", "last(awkward)")]
    slice_filter = EntityFilter(["my_entity_a", "my_entity_b"])

    expected_request = material_pb.CreateMaterializationRequest(
        **{
            "materialization": {
                "materialization_name": name,
                "expression": expression,
                "with_views": [
                    {"name": "my_second_view", "expression": "last(awkward)"}
                ],
                "destination": {
                    "object_store": {
                        "file_type": "FILE_TYPE_PARQUET",
                        "output_prefix_uri": "file:///prefix",
                    }
                },
                "slice": slice_filter.to_request(),
            }
        }
    )
    create_materialization(
        name,
        expression,
        destination,
        views,
        slice_filter=slice_filter,
        client=mockClient,
    )
    mockClient.materialization_stub.CreateMaterialization.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_create_materialization_redis_destination(mockClient):
    params = {
        "host_name": "my_host_name",
        "port": 1234,
        "use_tls": False,
        "database_number": 4321,
        "password": "my_password",
        "tls_cert": "my_tls_cert",
        "tls_key": "my_tls_key",
        "tls_ca_cert": "my_tls_ca_cert",
        "insecure_skip_verify": True,
    }

    redis_destination = RedisDestination(**params)

    name = "my_awkward_tacos"
    expression = "last(tacos)"
    destination = redis_destination
    views = [MaterializationView("my_second_view", "last(awkward)")]
    slice_filter = EntityFilter(["my_entity_a", "my_entity_b"])

    expected_request = material_pb.CreateMaterializationRequest(
        **{
            "materialization": {
                "materialization_name": name,
                "expression": expression,
                "with_views": [
                    {"name": "my_second_view", "expression": "last(awkward)"}
                ],
                "destination": {"redis": redis_destination.to_request()},
                "slice": slice_filter.to_request(),
            }
        }
    )
    create_materialization(
        name,
        expression,
        destination,
        views,
        slice_filter=slice_filter,
        client=mockClient,
    )
    mockClient.materialization_stub.CreateMaterialization.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_create_materialization_astra_streaming_destination(mockClient):
    params = {
        "tenant": "tenant",
        "namespace": "namespace",
        "topic_name": "name",
        "broker_service_url": "pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651",
        "auth_params": "token:xxxxx12345",
        "auth_plugin": "org.apache.pulsar.client.impl.auth.AuthenticationToken",
    }

    pulsar_destination = PulsarDestination(**params)

    name = "my_awkward_tacos"
    expression = "last(tacos)"
    destination = pulsar_destination
    views = [MaterializationView("my_second_view", "last(awkward)")]
    slice_filter = EntityFilter(["my_entity_a", "my_entity_b"])

    expected_request = material_pb.CreateMaterializationRequest(
        **{
            "materialization": {
                "materialization_name": name,
                "expression": expression,
                "with_views": [
                    {"name": "my_second_view", "expression": "last(awkward)"}
                ],
                "destination": {"pulsar": {"config": pulsar_destination.to_request()}},
                "slice": slice_filter.to_request(),
            }
        }
    )
    create_materialization(
        name,
        expression,
        destination,
        views,
        slice_filter=slice_filter,
        client=mockClient,
    )
    mockClient.materialization_stub.CreateMaterialization.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


def test_to_with_views_no_views_empty_list():
    expected = []
    assert to_with_views([]) == expected


def test_to_with_views_single_views_single_list():
    name = "my_name"
    expression = "my_expression"
    view = MaterializationView(name, expression)

    expected = [material_pb.WithView(name=name, expression=expression)]
    assert to_with_views([view]) == expected


def test_to_with_views_multiple_views_multiple_list():
    name = "my_name"
    expression = "my_expression"
    view = MaterializationView(name, expression)

    name2 = "my_name_2"
    expression2 = "my_expression_2"
    view2 = MaterializationView(name2, expression2)
    expected = [
        material_pb.WithView(name=name, expression=expression),
        material_pb.WithView(name=name2, expression=expression2),
    ]
    assert to_with_views([view, view2]) == expected
