import logging
import sys
from typing import Optional, Union

import grpc

import kaskada.kaskada.v1alpha.view_service_pb2 as view_pb
from kaskada.client import Client, get_client
from kaskada.utils import handleException, handleGrpcError

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


def get_view_name(
    view: Union[view_pb.View, view_pb.CreateViewResponse, view_pb.GetViewResponse, str]
) -> str:
    view_name = None
    if isinstance(view, view_pb.View):
        view_name = view.view_name
    elif isinstance(view, view_pb.CreateViewResponse) or isinstance(
        view, view_pb.GetViewResponse
    ):
        view_name = view.view.view_name
    elif isinstance(view, str):
        view_name = view

    if view_name is None:
        raise Exception(
            "invalid view parameter provided. \
                the view parameter must be the view object, view response from the SDK, or the view name"
        )

    return view_name


def list_views(
    search: Optional[str] = None, client: Optional[Client] = None
) -> view_pb.ListViewsResponse:
    """
    Lists all the views the user has access to

    Args:
        search (str, optional): The search parameter to filter list by. Defaults to None.
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        view_pb.ListViewsResponse: Response from the API
    """

    try:
        client = get_client(client)
        req = view_pb.ListViewsRequest(
            search=search,
        )
        logger.debug(f"List Views Request: {req}")
        return client.view_stub.ListViews(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def get_view(
    view: Union[view_pb.View, view_pb.CreateViewResponse, view_pb.GetViewResponse, str],
    client: Optional[Client] = None,
) -> view_pb.GetViewResponse:
    """
    Gets a view by name

    Args:
        view (Union[view_pb.View, view_pb.CreateViewResponse, view_pb.GetViewResponse, str]): The target view object
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        view_pb.GetViewResponse: Response from the API
    """

    try:
        view_name = get_view_name(view)
        client = get_client(client)
        req = view_pb.GetViewRequest(view_name=view_name)
        logger.debug(f"Get View Request: {req}")
        return client.view_stub.GetView(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def create_view(
    view_name: str, expression: str, client: Optional[Client] = None
) -> view_pb.CreateViewResponse:
    """
    Creates a view with a name and expression

    Args:
        view_name (str): The view name
        expression (str): The view fenl expression
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        view_pb.CreateViewResponse: Response from the API
    """

    try:
        client = get_client(client)
        req = view_pb.CreateViewRequest(
            view=view_pb.View(view_name=view_name, expression=expression)
        )
        logger.debug(f"Create View Request: {req}")
        return client.view_stub.CreateView(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)


def delete_view(
    view: Union[view_pb.View, view_pb.CreateViewResponse, view_pb.GetViewResponse, str],
    client: Optional[Client] = None,
    force: bool = False,
) -> view_pb.DeleteViewResponse:
    """
    Deletes a view

    Args:
        view (Union[view_pb.View, view_pb.CreateViewResponse, view_pb.GetViewResponse, str]): The target view object
        client (Client, optional): The Kaskada Client. Defaults to kaskada.KASKADA_DEFAULT_CLIENT.

    Returns:
        view_pb.DeleteViewResponse: Response from the API
    """

    try:
        view_name = get_view_name(view)
        client = get_client(client)
        req = view_pb.DeleteViewRequest(view_name=view_name, force=force)
        logger.debug(f"Delete View Request: {req}")
        return client.view_stub.DeleteView(req, metadata=client.get_metadata())
    except grpc.RpcError as e:
        handleGrpcError(e)
    except Exception as e:
        handleException(e)
