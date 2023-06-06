import os
from pathlib import Path
from unittest.mock import call, patch

import google.protobuf.wrappers_pb2 as wrappers
import pandas as pd
import pytest

import kaskada.client
import kaskada.kaskada.v1alpha.common_pb2 as common_pb
import kaskada.kaskada.v1alpha.pulsar_pb2 as pulsar_pb
import kaskada.kaskada.v1alpha.sources_pb2 as sources_pb
import kaskada.kaskada.v1alpha.table_service_pb2 as table_pb
import kaskada.table


@patch("kaskada.client.Client")
def test_table_create_table_default_values(mockClient):
    table_name = "test_table"
    time_column_name = "time"
    entity_key_column_name = "entity"
    expected_request = table_pb.CreateTableRequest(
        table=table_pb.Table(
            table_name=table_name,
            time_column_name=time_column_name,
            entity_key_column_name=entity_key_column_name,
        )
    )
    kaskada.table.create_table(
        table_name, time_column_name, entity_key_column_name, client=mockClient
    )
    mockClient.table_stub.CreateTable.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_create_table_grouping_id(mockClient):
    table_name = "test_table"
    time_column_name = "time"
    entity_key_column_name = "entity"
    grouping_id = "my_grouping_id"
    expected_request = table_pb.CreateTableRequest(
        table=table_pb.Table(
            table_name=table_name,
            time_column_name=time_column_name,
            entity_key_column_name=entity_key_column_name,
            grouping_id=grouping_id,
        )
    )
    kaskada.table.create_table(
        table_name,
        time_column_name,
        entity_key_column_name,
        grouping_id=grouping_id,
        client=mockClient,
    )
    mockClient.table_stub.CreateTable.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_create_table_subsort_column_id(mockClient):
    table_name = "test_table"
    time_column_name = "time"
    entity_key_column_name = "entity"
    subsort_column_name = "my_subsort_column"
    expected_request = table_pb.CreateTableRequest(
        table=table_pb.Table(
            table_name=table_name,
            time_column_name=time_column_name,
            entity_key_column_name=entity_key_column_name,
            subsort_column_name=wrappers.StringValue(value=subsort_column_name),
        )
    )
    kaskada.table.create_table(
        table_name,
        time_column_name,
        entity_key_column_name,
        subsort_column_name=subsort_column_name,
        client=mockClient,
    )
    mockClient.table_stub.CreateTable.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


def test_pulsar_table_source():
    broker_service_url = "pulsar://localhost:6650"
    admin_service_url = "https://some-service-url"
    auth_plugin = "auth-plugin"
    tenant = "test-tenant"
    namespace = "test-namespace"
    topic_name = "my-topic"
    test_source = kaskada.table.PulsarTableSource(
        broker_service_url,
        admin_service_url,
        auth_plugin,
        "",
        tenant,
        namespace,
        topic_name,
    )
    assert test_source._broker_service_url == broker_service_url
    assert test_source._auth_plugin == auth_plugin


@patch("kaskada.client.Client")
def test_table_create_table_with_pulsar_table_source(mockClient):
    table_name = "test_table"
    time_column_name = "time"
    entity_key_column_name = "entity"
    broker_service_url = "pulsar://localhost:6650"
    admin_service_url = "https://some-service-url"
    auth_plugin = "auth-plugin"
    tenant = "test-tenant"
    namespace = "test-namespace"
    topic_name = "my-topic"
    test_source = kaskada.table.PulsarTableSource(
        broker_service_url,
        admin_service_url,
        auth_plugin,
        "",
        tenant,
        namespace,
        topic_name,
    )
    expected_request = table_pb.CreateTableRequest(
        table=table_pb.Table(
            table_name=table_name,
            time_column_name=time_column_name,
            entity_key_column_name=entity_key_column_name,
            source=sources_pb.Source(
                pulsar=sources_pb.PulsarSource(
                    config=pulsar_pb.PulsarConfig(
                        **{
                            "broker_service_url": broker_service_url,
                            "admin_service_url": admin_service_url,
                            "auth_plugin": auth_plugin,
                            "tenant": tenant,
                            "namespace": namespace,
                            "topic_name": topic_name,
                        }
                    )
                )
            ),
        )
    )
    kaskada.table.create_table(
        table_name,
        time_column_name,
        entity_key_column_name,
        source=test_source,
        client=mockClient,
    )
    mockClient.table_stub.CreateTable.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_create_table_with_invalid_table_source(mockClient):
    table_name = "test_table"
    time_column_name = "time"
    entity_key_column_name = "entity"
    test_source = kaskada.table.TableSource()
    with pytest.raises(Exception):
        kaskada.table.create_table(
            table_name,
            time_column_name,
            entity_key_column_name,
            source=test_source,
            client=mockClient,
        )


@patch("kaskada.client.Client")
def test_table_load_parquet(mockClient):
    table_name = "test_table"
    local_file = "local.parquet"
    expected_request = table_pb.LoadDataRequest(
        table_name=table_name,
        file_input=common_pb.FileInput(
            file_type="FILE_TYPE_PARQUET", uri=f"file://{Path(local_file).absolute()}"
        ),
    )

    kaskada.table.load(table_name, local_file, client=mockClient)
    mockClient.table_stub.LoadData.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_load_parquet_s3(mockClient):
    table_name = "test_table"
    s3_file = "s3://my-bucket/my-prefix/myfile.parquet"
    expected_request = table_pb.LoadDataRequest(
        table_name=table_name,
        file_input=common_pb.FileInput(file_type="FILE_TYPE_PARQUET", uri=s3_file),
    )

    kaskada.table.load(table_name, s3_file, client=mockClient)
    mockClient.table_stub.LoadData.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_load_parquet_gcs(mockClient):
    table_name = "test_table"
    gcs_file = "gs://my-bucket/my-prefix/myfile.parquet"
    expected_request = table_pb.LoadDataRequest(
        table_name=table_name,
        file_input=common_pb.FileInput(file_type="FILE_TYPE_PARQUET", uri=gcs_file),
    )

    kaskada.table.load(table_name, gcs_file, client=mockClient)
    mockClient.table_stub.LoadData.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_load_csv(mockClient):
    table_name = "test_table"
    local_file = "local.csv"
    expected_request = table_pb.LoadDataRequest(
        table_name=table_name,
        file_input=common_pb.FileInput(
            file_type="FILE_TYPE_CSV", uri=f"file://{Path(local_file).absolute()}"
        ),
    )

    kaskada.table.load(table_name, local_file, client=mockClient)
    mockClient.table_stub.LoadData.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_load_csv_file_prefix(mockClient):
    table_name = "test_table"
    path = "local.csv"
    local_file = f"file://{path}"
    expected_request = table_pb.LoadDataRequest(
        table_name=table_name,
        file_input=common_pb.FileInput(
            file_type="FILE_TYPE_CSV", uri=f"file://{Path(path).absolute()}"
        ),
    )

    kaskada.table.load(table_name, local_file, client=mockClient)
    mockClient.table_stub.LoadData.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_load_from_absolute_path(mockClient):
    table_name = "test_table"
    path = "local.csv"
    local_file = f"file:///{path}"
    expected_request = table_pb.LoadDataRequest(
        table_name=table_name,
        file_input=common_pb.FileInput(
            file_type="FILE_TYPE_CSV", uri=f"file:///{path}"
        ),
    )

    kaskada.table.load(table_name, local_file, client=mockClient)
    mockClient.table_stub.LoadData.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_load_csv_azure(mockClient):
    table_name = "test_table"
    azure_file = "https://myaccount.blob.core.windows.net/mycontainer/myblob.csv"
    expected_request = table_pb.LoadDataRequest(
        table_name=table_name,
        file_input=common_pb.FileInput(file_type="FILE_TYPE_CSV", uri=azure_file),
    )

    kaskada.table.load(table_name, azure_file, client=mockClient)
    mockClient.table_stub.LoadData.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_table_load_invalid_type(mockClient):
    table_name = "test_table"
    local_file = "local.img"
    with pytest.raises(Exception):
        kaskada.table.load(table_name, local_file, client=mockClient)


@patch("kaskada.client.Client")
def test_table_load_dataframe(mockClient):
    table_name = "test_table"
    transactions_parquet = str(
        Path(__file__).parent.joinpath("transactions.parquet").absolute()
    )
    df = pd.read_parquet(transactions_parquet)
    expected_request = table_pb.LoadDataRequest(
        table_name=table_name,
        file_input=common_pb.FileInput(
            file_type="FILE_TYPE_CSV", uri=f"file://{transactions_parquet}"
        ),
    )
    kaskada.table.load_dataframe(table_name=table_name, dataframe=df, client=mockClient)
    assert mockClient.get_metadata.call_args_list == [call()]
    mockClient.table_stub.LoadData.assert_called_once()
