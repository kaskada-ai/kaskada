from pathlib import Path
from unittest.mock import patch

import pytest

import kaskada.client
import kaskada.kaskada.v1alpha.common_pb2 as common_pb
import kaskada.kaskada.v1alpha.destinations_pb2 as destinations_pb
import kaskada.kaskada.v1alpha.query_service_pb2 as query_pb
import kaskada.query

"""
def create_query(
    expression: str,
    result_behavior: str = "all-results",
    response_as: ResponseType = ResponseType.FILE_TYPE_PARQUET,
    data_token_id: Optional[str] = None,
    dry_run: bool = False,
    changed_since_time: Union[str, datetime.datetime, None] = None,
    final_result_time: Union[str, datetime.datetime, None] = None,
    limits: Optional[query_pb.Query.Limits] = None,
    slice_filter: Optional[SliceFilter] = None,
    experimental: bool = False,
    client: Optional[Client] = None,
) -> query_pb.CreateQueryResponse:
"""


@patch("kaskada.client.Client")
def test_create_query_with_defaults(mockClient):
    expression = "test_with_defaults"
    expected_request = query_pb.CreateQueryRequest(
        query=query_pb.Query(
            expression=expression,
            output_to={
                "object_store": destinations_pb.ObjectStoreDestination(
                    file_type=common_pb.FILE_TYPE_PARQUET
                )
            },
            result_behavior="RESULT_BEHAVIOR_ALL_RESULTS",
        ),
        query_options=query_pb.QueryOptions(presign_results=True),
    )

    kaskada.query.create_query(expression, client=mockClient)
    mockClient.query_stub.CreateQuery.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_get_query(mockClient):
    query_id = "12345"
    expected_request = query_pb.GetQueryRequest(query_id=query_id)

    kaskada.query.get_query(query_id, client=mockClient)
    mockClient.query_stub.GetQuery.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )


@patch("kaskada.client.Client")
def test_list_query_calls_stub(mockClient):
    expected_request = query_pb.ListQueriesRequest(search=None)

    kaskada.query.list_queries(client=mockClient)
    mockClient.query_stub.ListQueries.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )

    search = "tacos"
    expected_request = query_pb.ListQueriesRequest(search=search)

    kaskada.query.list_queries(search=search, client=mockClient)
    mockClient.query_stub.ListQueries.assert_called_with(
        expected_request, metadata=mockClient.get_metadata()
    )
