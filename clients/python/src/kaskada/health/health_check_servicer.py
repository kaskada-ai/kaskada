import logging
import threading
from typing import Dict, Optional

from grpc.health.v1 import health_pb2, health_pb2_grpc

from kaskada.health.health_check_client import (
    HealthCheckClient,
    HealthCheckClientFactory,
)

logger = logging.getLogger(__name__)


class HealthCheckServicer:
    """An aggregate health check servicer designed to monitor the health of multiple gRPC services by managing multiple HealthCheckClients.

    The underlying implementation uses thread mutex locks to ensure safe updating/reading of health service state.
    """

    def __init__(
        self, client_factory: HealthCheckClientFactory = HealthCheckClientFactory()
    ):
        """Instantiates a HealthCheckServicer."""
        self.__states: Dict[str, health_pb2.HealthCheckResponse.ServingStatus] = {}
        self.__state_lock = threading.Lock()
        self.__health_check_clients: Dict[str, HealthCheckClient] = {}
        self.__client_factory = client_factory

    def add_service(
        self, service_name: str, health_check_endpoint: str, is_secure: bool
    ):
        """Adds a gRPC service to monitor.

        Args:
            service_name (str): the name of the service
            health_check_endpoint (str): the service's health check endpoint
            is_secure (bool): True to use SSL or False to use an insecure connection
        """
        self.__state_lock.acquire()
        try:
            self.__health_check_clients[
                service_name
            ] = self.__client_factory.get_client(health_check_endpoint, is_secure)
            self.__states[
                service_name
            ] = health_pb2.HealthCheckResponse.ServingStatus.SERVING
            logger.debug(f"Added service: {service_name} to health check clients")
        finally:
            self.__state_lock.release()

    def check(self) -> health_pb2.HealthCheckResponse.ServingStatus:
        """Checks the status of all services managed by the servicer.

        Returns:
            health_pb2.HealthCheckResponse.ServingStatus: SERVING if all are ready, otherwise a non-ready status
        """
        self.__state_lock.acquire()
        try:
            if len(self.__health_check_clients) == 0:
                logger.debug(f"No clients to check health. Defaulting to SERVING.")
                return health_pb2.HealthCheckResponse.ServingStatus.SERVING
        finally:
            self.__state_lock.release()

        status = health_pb2.HealthCheckResponse.ServingStatus.SERVING
        for service_name, client in self.__health_check_clients.items():
            service_status = client.check()
            self.__state_lock.acquire()
            self.__states[service_name] = service_status
            self.__state_lock.release()
            if service_status != health_pb2.HealthCheckResponse.ServingStatus.SERVING:
                logger.debug(
                    f"service: {service_name} reported status: {service_status}"
                )
                status = service_status
        return status

    def get(
        self, service_name: Optional[str] = None
    ) -> health_pb2.HealthCheckResponse.ServingStatus:
        """Gets the overall status of the servicer or of a specific service.
        This method pulls from a thread-safe cache rather than performing the actual health check.
        To get the service status, call the check() method.

        Args:
            service_name (Optional[str], optional): service to return the status. None will aggregate the status Defaults to None.

        Returns:
            health_pb2.HealthCheckResponse.ServingStatus: SERVING if all services are reporting ready, otherwise a non-ready status
        """
        self.__state_lock.acquire()
        try:
            if service_name is None:
                status = health_pb2.HealthCheckResponse.ServingStatus.SERVING
                for service_name, service_status in self.__states.items():
                    self.__states[service_name] = service_status
                    if (
                        service_status
                        != health_pb2.HealthCheckResponse.ServingStatus.SERVING
                    ):
                        logger.debug(
                            f"service: {service_name} reported status: {service_status}"
                        )
                        status = service_status
                return status
            else:
                return self.__states[service_name]
        finally:
            self.__state_lock.release()
