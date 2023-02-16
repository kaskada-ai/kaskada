import datetime
from enum import Enum
from typing import List, Optional, Union

import grpc

import kaskada.formatters
import kaskada.kaskada.v1alpha.materialization_service_pb2 as materialization_pb
import kaskada.kaskada.v1alpha.query_service_pb2 as query_pb
from kaskada.client import (
    KASKADA_DEFAULT_CLIENT,
    KASKADA_DEFAULT_SLICE,
    Client,
    get_client,
)
from kaskada.slice_filters import SliceFilter
from kaskada.utils import get_timestamp, handleException, handleGrpcError


class ResponseType(Enum):
    FILE_TYPE_PARQUET = 1
    FILE_TYPE_CSV = 2


def queryV2(
    query: str,
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
    Performs a query

    Args:
        query (str): The query to perform
        result_behavior (str, optional):
            Determines which results are returned. Either "all-results" (default), or "final-results" which returns
            only the final values for each entity.
        responsed_as (ResponseType):
            Determines how the response is returned.  Either "parquet" (default) or "csv".
        data_token_id (str, optional):
            Enables repeatable queries. Queries performed against the same dataToken are always ran the same input data.
        dry_run(bool, optional):
            When `True`, the query is validated and if there are no errors, the resultant schema is returned.
            No actual computation of results is performed.
        changed_since_time (datetime.datetime, optional):
            Configure the inclusive datetime after which results will be output.
        limits (pb.QueryRequest.Limits, optional):
            Configure limits on the output set.
        slice_filter (SliceFilter, optional):
            Enables slice filter. Currently, only slice entity percent filters are supported. Defaults to None.
        experimental (bool):
            When `True`, then experimental features are allowed. Data returned when using this flag is not
            guaranteed to be correct. Default to False
        client (Client, optional):
            The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        compute_pb.QueryResponse: Response from the API
    """
    if slice_filter is None:
        slice_filter = KASKADA_DEFAULT_SLICE

    change_since_time = get_timestamp(changed_since_time)
    final_result_time = get_timestamp(final_result_time)

    try:
        client = get_client(client)
        query_options = {
            "dry_run": dry_run,
            "experimental_features": experimental,
            "stream_metrics": False,
            "presign_results": True,
        }

        query_request = {
            "expression": query,
            "data_token_id": data_token_id,
            "changed_since_time": change_since_time,
            "final_result_time": final_result_time,
            "limits": limits,
        }

        if result_behavior == "final-results":
            query_request["result_behavior"] = "RESULT_BEHAVIOR_FINAL_RESULTS"
            if final_result_time is not None:
                query_request[
                    "result_behavior"
                ] = "RESULT_BEHAVIOR_FINAL_RESULTS_AT_TIME"
        else:
            query_request["result_behavior"] = "RESULT_BEHAVIOR_ALL_RESULTS"

        query_request["as_files"] = {"file_type": response_as.name}

        if slice_filter is not None:
            query_request["slice"] = slice_filter.to_request()

        in_ipython = kaskada.formatters.in_ipython()
        if in_ipython:
            query_options["stream_metrics"] = True

        request_args = {"query": query_request, "query_options": query_options}
        request = query_pb.CreateQueryRequest(**request_args)
        return execute_query(request, client)
    except grpc.RpcError as exec:
        handleGrpcError(exec)
    except Exception as exec:
        handleException(exec)


def execute_query(
    request: query_pb.CreateQueryRequest, client: Client
) -> query_pb.CreateQueryResponse:
    """Executes a query using the streaming request format"""
    response = query_pb.CreateQueryResponse()
    in_ipython = kaskada.formatters.in_ipython()
    if in_ipython:
        from IPython.display import clear_output, display
    responses = client.query_stub.CreateQuery(request, metadata=client.get_metadata())
    for resp in responses:
        response.MergeFrom(resp)
        if in_ipython:
            clear_output(wait=True)
            display(response)

    if in_ipython:
        clear_output(wait=True)
    return response
