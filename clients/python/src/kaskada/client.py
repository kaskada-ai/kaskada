import logging
import sys
import traceback
from typing import List, Optional, Tuple

import certifi
import grpc
import IPython

import kaskada.formatters
import kaskada.kaskada.v1alpha.materialization_service_pb2_grpc as mat_grpc
import kaskada.kaskada.v1alpha.query_service_pb2_grpc as query_grpc
import kaskada.kaskada.v1alpha.table_service_pb2_grpc as table_grpc
import kaskada.kaskada.v1alpha.view_service_pb2_grpc as view_grpc
from kaskada.slice_filters import SliceFilter

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

# Module global client configured at initialization time. By default, all
# API calls utilize the global client config
KASKADA_DEFAULT_CLIENT = None
# The slicing parameter passed to query by default. By default, this is None.
KASKADA_DEFAULT_SLICE = None
# The default API server is localhost.
KASKADA_DEFAULT_ENDPOINT = "localhost:50051"
# The default local API server does not use a secure connection
KASKADA_IS_SECURE = False


def showtraceback(_self, *_args, **_kwargs):
    """Upates the IPython interactive shell to show tracebook"""
    traceback_lines = traceback.format_exception(*sys.exc_info())
    message = traceback_lines[-1]
    sys.stderr.write(message)


# fmt: off
IPython.core.interactiveshell.InteractiveShell.showtraceback = showtraceback # type:ignore
# fmt: on


def init(
    client_id: Optional[str] = None,
    endpoint: Optional[str] = KASKADA_DEFAULT_ENDPOINT,
    is_secure: bool = KASKADA_IS_SECURE,
):
    """
    Initializes the Kaskada library with a client. Creating a client will update the default module default client.

    Args:
        client_id (str, optional): Kaskada Client ID.
        endpoint (str, optional): API Endpoint. Defaults to KASKADA_DEFAULT_ENDPOINT/localhost:50051.
        is_secure (bool): Use SSL connection. Defaults to False.
    Returns:
        Client: Kaskada Client
    """
    kaskada.formatters.try_init()
    logger.debug(
        f"Client initialized with client_id: {client_id}, endpoint: {endpoint}, is_secure: {is_secure}"
    )
    global KASKADA_DEFAULT_CLIENT
    KASKADA_DEFAULT_CLIENT = Client(
        client_id=client_id,
        endpoint=endpoint,
        is_secure=is_secure,
    )


def set_default_slice(slice: SliceFilter):
    """
    Sets the default slice used in every query

    Args:
        slice (SliceFilter): SliceFilter to set the default
    """
    logger.debug(f"Default slice set to type {type(slice)}")

    global KASKADA_DEFAULT_SLICE
    KASKADA_DEFAULT_SLICE = slice


class Client(object):
    def __init__(
        self,
        client_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        is_secure: bool = True,
    ):
        if is_secure:
            with open(certifi.where(), "rb") as f:
                trusted_certs = f.read()
            credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)
            channel = grpc.secure_channel(endpoint, credentials)
        else:
            channel = grpc.insecure_channel(endpoint)
        self.table_stub = table_grpc.TableServiceStub(channel)
        self.view_stub = view_grpc.ViewServiceStub(channel)
        self.query_stub = query_grpc.QueryServiceStub(channel)
        self.materialization_stub = mat_grpc.MaterializationServiceStub(channel)
        self.client_id = client_id

    def get_metadata(self) -> List[Tuple[str, str]]:
        """
        Fetches the metadata for the current client. Renews token if necessary.

        Raises:
            Exception: invalid token

        Returns:
            List[Tuple[str, str]]: Client metadata
        """
        metadata: List[Tuple[str, str]] = []
        if self.client_id is not None:
            metadata.append(("client-id", self.client_id))
        return metadata


def validate_client(client: Client):
    """Valides the client by checking the service stubs

    Args:
        client (Client): The client to validate

    Raises:
        ValueError: View service stub was not initialized properly.
        ValueError: Table service stubs was not initialized properly.
        ValueError: Query service stubs was not initialized properly.
        ValueError: Materialization service stubs was not initialized properly.
    """
    if client.view_stub is None:
        raise ValueError(
            "Invalid client provided. View service stub was not initialized properly."
        )
    if client.table_stub is None:
        raise ValueError(
            "Invalid client provided. Table service stubs was not initialized properly."
        )
    if client.query_stub is None:
        raise ValueError(
            "Invalid client provided. Query service stubs was not initialized properly."
        )
    if client.materialization_stub is None:
        raise ValueError(
            "Invalid client provided. Materialization service stubs was not initialized properly."
        )


def get_client(client: Optional[Client] = None) -> Client:
    """Gets and validates the current client. If no client is provided, the global client is returned.

    Args:
        client (Optional[Client], optional): An optional client to verify Defaults to the global client.

    Raises:
        ValueError: No client is initialized or client is improperly initialized

    Returns:
        Client: The provided client argument or the global client.
    """
    if client is None and KASKADA_DEFAULT_CLIENT is None:
        raise ValueError("Client must be provided")
    elif client is None:
        client = KASKADA_DEFAULT_CLIENT

    if client is None:
        raise ValueError("Client must be provided")
    validate_client(client)
    return client
