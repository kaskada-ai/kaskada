import logging
import sys
import time
import traceback
from typing import List, Optional, Tuple

import certifi
import grpc
import IPython
from grpc.health.v1 import health_pb2

import kaskada.formatters
import kaskada.kaskada.v1alpha.materialization_service_pb2_grpc as mat_grpc
import kaskada.kaskada.v1alpha.query_service_pb2_grpc as query_grpc
import kaskada.kaskada.v1alpha.table_service_pb2_grpc as table_grpc
import kaskada.kaskada.v1alpha.view_service_pb2_grpc as view_grpc
from kaskada.health.health_check_client import (
    HealthCheckClient,
    HealthCheckClientFactory,
)
from kaskada.health.health_check_servicer import HealthCheckServicer
from kaskada.health.health_check_watcher import HealthCheckWatcher
from kaskada.slice_filters import SliceFilter

logger = logging.getLogger(__name__)

# The default API server is localhost.
KASKADA_DEFAULT_ENDPOINT = "localhost:50051"
# The default manager API health check endpoint.
KASKADA_MANAGER_DEFAULT_HEALTH_CHECK_ENDPOINT = "localhost:6666"
# The default engine API health check endpoint
KASKADA_ENGINE_DEFAULT_HEALTH_CHECK_ENDPOINT = "localhost:50052"
# The default local API server does not use a secure connection
KASKADA_IS_SECURE = False
# The maximum amount of times to perform a health check
MAX_HEALTH_CHECK_ATTEMPTS = 10

kaskada.formatters.try_init()


class ClientFactory:
    def __init__(
        self,
        client_id: Optional[str],
        endpoint: str = KASKADA_DEFAULT_ENDPOINT,
        is_secure: bool = True,
        polling_interval_seconds: float = 1,
    ):
        """Instantiates a client factory.

        Args:
            client_id (Optional[str]): A configurable client ID.
            endpoint (str, optional): The endpoint of the Manager service. Defaults to KASKADA_DEFAULT_ENDPOINT.
            is_secure (bool, optional): True to use TLS or False to use insecure connection. Defaults to True.
            polling_interval_seconds (float, optional): The number of seconds to wait between readiness polls. Defaults to 0.5.
        """
        self.client_id = client_id
        self.endpoint = endpoint
        self.is_secure = is_secure
        self.polling_interval_seconds = polling_interval_seconds

    def get_client(self, should_connect: bool = True, should_check_health: bool = True):
        """Gets a client.

        Args:
            should_connect (bool, optional): True to connect. False to not connect. Defaults to True.
            should_check_health (bool, optional): True to check health. False to not check. Defaults to True.
        """
        return Client(
            client_id=self.client_id,
            endpoint=self.endpoint,
            is_secure=self.is_secure,
            should_check_health=should_check_health,
            polling_interval_seconds=self.polling_interval_seconds,
            should_connect=should_connect,
        )


class Client(object):
    """A Kaskada Client connects to the Kaskada Manager API"""

    health_check_watcher: Optional[HealthCheckServicer] = None

    def __init__(
        self,
        client_id: Optional[str],
        endpoint: str,
        is_secure: bool,
        should_check_health: bool = True,
        polling_interval_seconds: float = 1.0,
        should_connect: bool = True,
    ):
        """Instantiates a Kaskada Client

        Args:
            client_id (Optional[str], optional): A configurable client ID.
            endpoint (str): The endpoint of the Manager service.
            is_secure (bool): True to use TLS or False to use insecure connection.
            should_check_health (bool, optional): True to check the health of the services. Defaults to True.
            polling_interval_seconds (float, optional): The number of seconds to wait between readiness polls. Defaults to 1.0 seconds.
            should_connect (bool, optional): True to connect automatically or False to not connect. Defaults to True.
        """
        self.client_id = client_id
        self.endpoint = endpoint
        self.is_secure = is_secure
        self.polling_interval_seconds = polling_interval_seconds
        self.should_check_health = should_check_health
        if should_connect:
            self.connect()

    def connect(self):
        """Attempts to connect to the manager service. The connection tries up to MAX_HEALTH_CHECK_ATTEMPTS before throwing a ConnectionError.
        The connection checks the health endpoint of the manager service until reporting SERVING.

        Raises:
            ConnectionError: if unable to connect after MAX_HEALTH_CHECK_ATTEMPTS
        """
        if self.should_check_health:
            health_factory = HealthCheckClientFactory()
            for i in range(0, MAX_HEALTH_CHECK_ATTEMPTS + 1):
                if i == MAX_HEALTH_CHECK_ATTEMPTS:
                    raise ConnectionError(
                        f"Unable to connect after {MAX_HEALTH_CHECK_ATTEMPTS} attempts"
                    )
                manager_health = health_factory.get_client(
                    KASKADA_MANAGER_DEFAULT_HEALTH_CHECK_ENDPOINT, KASKADA_IS_SECURE
                ).check()
                engine_health = health_factory.get_client(
                    KASKADA_ENGINE_DEFAULT_HEALTH_CHECK_ENDPOINT, KASKADA_IS_SECURE
                ).check()
                if (
                    manager_health
                    == engine_health
                    == health_pb2.HealthCheckResponse.ServingStatus.SERVING
                ):
                    logger.info("Successfully connected.")
                    break
                time.sleep(self.polling_interval_seconds)

            self.health_servicer = HealthCheckServicer()
            self.health_servicer.add_service(
                "manager", KASKADA_MANAGER_DEFAULT_HEALTH_CHECK_ENDPOINT, False
            )
            self.health_servicer.add_service(
                "engine", KASKADA_ENGINE_DEFAULT_HEALTH_CHECK_ENDPOINT, False
            )

        if self.is_secure:
            with open(certifi.where(), "rb") as f:
                trusted_certs = f.read()
            credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)
            self.channel = grpc.secure_channel(self.endpoint, credentials)
        else:
            self.channel = grpc.insecure_channel(self.endpoint)

        self.table_stub = table_grpc.TableServiceStub(self.channel)
        self.view_stub = view_grpc.ViewServiceStub(self.channel)
        self.query_stub = query_grpc.QueryServiceStub(self.channel)
        self.materialization_stub = mat_grpc.MaterializationServiceStub(self.channel)

        if self.should_check_health:
            self.health_check_watcher = HealthCheckWatcher(self.health_servicer)
            self.health_check_watcher.start()

    def disconnect(self):
        """Stops the client."""
        if self.should_check_health and self.health_check_watcher is not None:
            self.health_check_watcher.stop()
            self.health_check_watcher.join()
        self.channel.close()

    def is_ready(self, always_check: bool = False) -> bool:
        """Determines if the client is ready by querying the health servicer. If should_check_health is set to False, this method always returns True.

        Args:
            always_check (bool, optional): True to always fetch from the health servicer. False to use the latest cached value. Defaults to False.

        Returns:
            bool: True if ready for traffic. False if not ready for traffic.
        """
        if not self.should_check_health:
            return True

        if self.health_check_watcher is None:
            return False

        if always_check:
            self.health_servicer.check()
        return (
            self.health_servicer.get()
            == health_pb2.HealthCheckResponse.ServingStatus.SERVING
        )

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

    def validate(self):
        """Validates the client by checking the service stubs

        Raises:
            ValueError: View service stub was not initialized properly.
            ValueError: Table service stubs was not initialized properly.
            ValueError: Query service stubs was not initialized properly.
            ValueError: Materialization service stubs was not initialized properly.
        """
        if not self.is_ready():
            raise RuntimeError(
                "Kaskada services are not available. If this is a local session, please restart the session."
            )
        if self.view_stub is None:
            raise ValueError(
                "Invalid client provided. View service stub was not initialized properly."
            )
        if self.table_stub is None:
            raise ValueError(
                "Invalid client provided. Table service stubs was not initialized properly."
            )
        if self.query_stub is None:
            raise ValueError(
                "Invalid client provided. Query service stubs was not initialized properly."
            )
        if self.materialization_stub is None:
            raise ValueError(
                "Invalid client provided. Materialization service stubs was not initialized properly."
            )


# Module global client configured at initialization time. By default, all
# API calls utilize the global client config
KASKADA_DEFAULT_CLIENT: Optional[Client] = None
# The slicing parameter passed to query by default. By default, this is None.
KASKADA_DEFAULT_SLICE = None


def showtraceback(_self, *_args, **_kwargs):
    """Upates the IPython interactive shell to show tracebook"""
    traceback_lines = traceback.format_exception(*sys.exc_info())
    message = traceback_lines[-1]
    sys.stderr.write(message)


# fmt: off
IPython.core.interactiveshell.InteractiveShell.showtraceback = showtraceback # type:ignore
# fmt: on


def reset():
    """Resets the global variables for the client module."""
    global KASKADA_DEFAULT_CLIENT
    KASKADA_DEFAULT_CLIENT = None
    global KASKADA_DEFAULT_SLICE
    KASKADA_DEFAULT_SLICE = None


def set_default_slice(slice: SliceFilter):
    """Sets the default slice used in every query

    Args:
        slice (SliceFilter): SliceFilter to set the default
    """
    logger.debug(f"Default slice set to type {type(slice)}")

    global KASKADA_DEFAULT_SLICE
    KASKADA_DEFAULT_SLICE = slice


def set_default_client(client: Client):
    """Sets the default client used in every query

    Args:
        client (Client): the target client
    """
    logger.debug(f"Default client set to: {client.endpoint}")
    global KASKADA_DEFAULT_CLIENT
    KASKADA_DEFAULT_CLIENT = client


def get_client(client: Optional[Client] = None, validate: bool = True) -> Client:
    """Gets and validates the current client. If no client is provided, the global client is returned.

    Args:
        client (Optional[Client], optional): An optional client to verify Defaults to the global client.

    Raises:
        ValueError: No client is initialized or client is improperly initialized

    Returns:
        Client: The provided client argument or the global client.
    """
    if client is None and KASKADA_DEFAULT_CLIENT is None:
        raise ValueError(
            "No client was provided, and no global client is configured. Consider setting the global client by creating a session, for example 'kaskada.api.session.LocalBuilder().build()'."
        )
    elif client is None:
        client = KASKADA_DEFAULT_CLIENT

    assert client is not None  # make mypy happy
    if validate:
        client.validate()
    return client
