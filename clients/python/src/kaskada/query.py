import datetime
import logging
import sys
from enum import Enum
from typing import List, Optional, Union

import grpc
from google.protobuf.message import Message

import kaskada.formatters
import kaskada.kaskada.v1alpha.destinations_pb2 as destinations_pb
import kaskada.kaskada.v1alpha.query_service_pb2 as query_pb
from kaskada.client import KASKADA_DEFAULT_SLICE, Client, get_client
from kaskada.slice_filters import SliceFilter
from kaskada.utils import get_timestamp, handleException, handleGrpcError

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryResource(object):
    CURRENT_DATA_TOKEN = ""

    def __init__(self, query: query_pb.Query):
        self.query = query

    def to_query_request(self):
        result = {
            "query_id": self.query.query_id,
            "expression": self.query.expression,
            "data_token_id": self.query.data_token_id,
            "changed_since_time": self.query.changed_since_time,
            "final_result_time": self.query.final_result_time,
            "limits": self.query.limits,
            "result_behavior": self.query.result_behavior,
        }
        if isinstance(self.query, Message):
            if self.query.HasField("output_to"):
                result["output_to"] = self.query.output_to

        return result

    def run(
        self,
        data_token=None,
        dry_run=False,
        experimental_features=False,
        client: Optional[Client] = None,
    ):
        try:
            client = get_client(client)
            query_options = {
                "dry_run": dry_run,
                "experimental_features": experimental_features,
                "stream_metrics": False,
                "presign_results": True,
            }
            query_request = self.to_query_request()
            in_ipython = kaskada.formatters.in_ipython()
            if in_ipython:
                query_options["stream_metrics"] = True
            if data_token is not None:
                query_request["data_token_id"] = {"value": data_token}
            request_args = {
                "query": query_request,
                "query_options": query_options,
            }
            request = query_pb.CreateQueryRequest(**request_args)
            logger.debug(f"Create Query Request: {request}")
            return execute_create_query(request, client)
        except grpc.RpcError as e:
            handleGrpcError(e)
        except Exception as e:
            handleException(e)

    def run_with_latest(self, dry_run=False, experimental_features=False):
        return self.run(
            data_token=QueryResource.CURRENT_DATA_TOKEN,
            dry_run=dry_run,
            experimental_features=experimental_features,
        )


class ResponseType(Enum):
    FILE_TYPE_PARQUET = 1
    FILE_TYPE_CSV = 2


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
    Performs a query

    Args:
        expression (str): A Fenl expression to compute
        result_behavior (str, optional):
            Determines which results are returned. Either "all-results" (default), or "final-results" which returns
            only the final values for each entity.
        responsed_as (ResponseType):
            Determines how the response is returned.  Either "parquet" (default) or "csv".
        data_token_id (str, optional):
            Enables repeatable queries. Queries performed against the same dataToken are always run against the same input data.
        dry_run(bool, optional):
            When `True`, the query is validated and if there are no errors, the resultant schema is returned.
            No actual computation of results is performed.
        changed_since_time (datetime.datetime, optional):
            Time bound (inclusive) after which results will be output.
        limits (pb.QueryRequest.Limits, optional):
            Configure limits on the output set.
        slice_filter (SliceFilter, optional):
            How to slice the input data for the query
        experimental (bool):
            When `True`, then experimental features are allowed. Data returned when using this flag is not
            guaranteed to be correct. Default to False
        client (Client, optional):
            The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        query_pb.CreateQueryResponse
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
            "expression": expression,
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

        destination_args = {"file_type": response_as.name}
        destination = destinations_pb.ObjectStoreDestination(**destination_args)
        query_request["output_to"] = {"object_store": destination}

        if slice_filter is not None:
            query_request["slice"] = slice_filter.to_request()

        in_ipython = kaskada.formatters.in_ipython()
        if in_ipython:
            query_options["stream_metrics"] = True

        request_args = {"query": query_request, "query_options": query_options}
        request = query_pb.CreateQueryRequest(**request_args)
        logger.debug(f"Query Request: {request}")
        return execute_create_query(request, client)
    except grpc.RpcError as exec:
        handleGrpcError(exec)
    except Exception as exec:
        handleException(exec)


def execute_create_query(
    request: query_pb.CreateQueryRequest, client: Client
) -> query_pb.CreateQueryResponse:
    """Executes a create query request using the streaming request format"""
    response = query_pb.CreateQueryResponse()
    in_ipython = kaskada.formatters.in_ipython()
    if in_ipython:
        from IPython.display import clear_output, display
    responses = client.query_stub.CreateQuery(request, metadata=client.get_metadata())
    for resp in responses:
        response.MergeFrom(resp)
        logger.debug(f"Query Response: {response}")
        if in_ipython:
            clear_output(wait=True)
            display(response)

    if in_ipython:
        clear_output(wait=True)
    return response


def get_query(query_id: str, client: Optional[Client] = None):
    """
    Gets a query by query ID
    Args:
        query_id (str): The target query ID
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Raises:
        NotImplementedError
    Returns:
        query_pb.GetQueryResponse
    """
    try:
        client = get_client(client)
        req = query_pb.GetQueryRequest(
            query_id=query_id,
        )
        logger.debug(f"Get Query Request: {req}")
        resp = client.query_stub.GetQuery(req, metadata=client.get_metadata())
        return QueryResource(resp.query)
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def list_queries(search: Optional[str] = None, client: Optional[Client] = None):
    """
    Lists all queries the user has previously performed

    Args:
        search (str): The search parameter to filter queries by. Defaults to None.
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Raises:
        NotImplementedError
    Returns:
        query_pb.ListQueryResponse
    """
    try:
        client = get_client(client)
        req = query_pb.ListQueriesRequest(
            search=search,
        )
        logger.debug(f"List Query Request: {req}")
        return client.query_stub.ListQueries(req, metadata=client.get_metadata())

    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)
