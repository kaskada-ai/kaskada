from typing import Optional

import grpc
from google.protobuf.message import Message

import kaskada.compute
import kaskada.formatters
import kaskada.kaskada.v1alpha.query_service_pb2 as query_pb
from kaskada.client import KASKADA_DEFAULT_CLIENT, Client, get_client
from kaskada.utils import handleException, handleGrpcError


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
            if self.query.HasField("as_files"):
                result["as_files"] = self.query.as_files

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
            return kaskada.compute.execute_query(request, client)
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


def get_query(query_id: str, client: Optional[Client] = None):
    """
    Gets a query by query ID
    Args:
        query_id (str): The target query ID
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Raises:
        NotImplementedError
    """
    try:
        client = get_client(client)
        req = query_pb.GetQueryRequest(
            query_id=query_id,
        )
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
    """
    try:
        client = get_client(client)
        req = query_pb.ListQueriesRequest(
            search=search,
        )
        return client.query_stub.ListQueries(req, metadata=client.get_metadata())

    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)
