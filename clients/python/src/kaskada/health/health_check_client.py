import certifi
import grpc
from grpc.health.v1 import health_pb2, health_pb2_grpc


class HealthCheckClient:
    """A generic health check client designed to poll the gRPC health check endpoint of a service."""

    def __init__(self, health_check_endpoint: str, is_secure: bool):
        """Instantiate a Health Check Client

        Args:
            health_check_endpoint (str): gRRPC health check endpoint
            is_secure (bool): True to use SSL or False to use an insecure channel
        """
        if is_secure:
            with open(certifi.where(), "rb") as f:
                trusted_certs = f.read()
            credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)
            self.channel = grpc.secure_channel(health_check_endpoint, credentials)
        else:
            self.channel = grpc.insecure_channel(health_check_endpoint)
        self.health_check_stub = health_pb2_grpc.HealthStub(self.channel)

    def __del__(self):
        """Garbage collector to clean up channel resources"""
        if self.channel is not None:
            self.channel.close()

    def check(self) -> health_pb2.HealthCheckResponse.ServingStatus:
        """Synchronously checks the health of the server by calling the Check rpc. If the server is not reachable, the result will be UNKNOWN.

        Returns:
            health_pb2.HealthCheckResponse.ServingStatus: status reported by the health check
        """
        try:
            return self.health_check_stub.Check(health_pb2.HealthCheckRequest()).status
        except Exception:
            return health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN


class HealthCheckClientFactory:
    def __init__(self):
        pass

    def get_client(
        self, health_check_endpoint: str, is_secure: bool
    ) -> HealthCheckClient:
        return HealthCheckClient(health_check_endpoint, is_secure)
