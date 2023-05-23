from unittest.mock import MagicMock, call, patch

import pytest
from grpc.health.v1 import health_pb2

from kaskada.health.health_check_servicer import HealthCheckServicer


@patch("kaskada.health.health_check_client.HealthCheckClientFactory")
@patch("kaskada.health.health_check_client.HealthCheckClient")
def test_health_check_servicer_check_successful(client_factory, client):
    client_factory.get_client = MagicMock(return_value=client)
    client.check = MagicMock(
        return_value=health_pb2.HealthCheckResponse.ServingStatus.SERVING
    )
    servicer = HealthCheckServicer(client_factory=client_factory)
    servicer.add_service("test_service", "test_endpoint", True)
    assert servicer.check() == health_pb2.HealthCheckResponse.ServingStatus.SERVING
    client_factory.get_client.assert_called_once_with("test_endpoint", True)
    client.check.assert_called_once()


@patch("kaskada.health.health_check_client.HealthCheckClientFactory")
@patch("kaskada.health.health_check_client.HealthCheckClient")
def test_health_check_servicer_check_unknown_status(client_factory, client):
    client_factory.get_client = MagicMock(return_value=client)
    client.check = MagicMock(
        return_value=health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
    )
    servicer = HealthCheckServicer(client_factory=client_factory)
    servicer.add_service("test_service", "test_endpoint", True)
    assert servicer.check() == health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
    client_factory.get_client.assert_called_once_with("test_endpoint", True)
    client.check.assert_called_once()


@patch("kaskada.health.health_check_client.HealthCheckClientFactory")
@patch("kaskada.health.health_check_client.HealthCheckClient")
@patch("kaskada.health.health_check_client.HealthCheckClient")
def test_health_check_servicer_check_multiple_clients_successful(
    client_factory, client, client_2
):
    client_factory.get_client.side_effect = [client, client_2]
    client.check = MagicMock(
        return_value=health_pb2.HealthCheckResponse.ServingStatus.SERVING
    )
    client_2.check = MagicMock(
        return_value=health_pb2.HealthCheckResponse.ServingStatus.SERVING
    )

    servicer = HealthCheckServicer(client_factory=client_factory)
    servicer.add_service("test_service", "test_endpoint", True)
    servicer.add_service("test_service_2", "test_endpoint_2", False)
    assert servicer.check() == health_pb2.HealthCheckResponse.ServingStatus.SERVING
    client_factory.get_client.assert_has_calls(
        [call("test_endpoint", True), call("test_endpoint_2", False)]
    )
    client.check.assert_called_once()
    client_2.check.assert_called_once()


@patch("kaskada.health.health_check_client.HealthCheckClientFactory")
@patch("kaskada.health.health_check_client.HealthCheckClient")
@patch("kaskada.health.health_check_client.HealthCheckClient")
def test_health_check_servicer_multiple_clients_one_unsuccessful(
    client_factory, client, client_2
):
    """Tests the servicer with multiple clients where only one reports not ready/UNKNOWN.
    The servicer's job is to aggregate the worst case health scenario and should report with the get method.
    """
    client_factory.get_client.side_effect = [client, client_2]
    client.check = MagicMock(
        return_value=health_pb2.HealthCheckResponse.ServingStatus.SERVING
    )
    client_2.check = MagicMock(
        return_value=health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
    )

    servicer = HealthCheckServicer(client_factory=client_factory)
    servicer.add_service("test_service", "test_endpoint", True)
    servicer.add_service("test_service_2", "test_endpoint_2", False)
    assert servicer.check() == health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
    client_factory.get_client.assert_has_calls(
        [call("test_endpoint", True), call("test_endpoint_2", False)]
    )
    client.check.assert_called_once()
    client_2.check.assert_called_once()
    assert servicer.get() == health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
    assert (
        servicer.get(service_name="test_service")
        == health_pb2.HealthCheckResponse.ServingStatus.SERVING
    )
    assert (
        servicer.get(service_name="test_service_2")
        == health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
    )


@patch("kaskada.health.health_check_client.HealthCheckClientFactory")
def test_health_check_servicer_get_successful(client_factory):
    """By default the health check servicer should return successful/serving."""
    servicer = HealthCheckServicer(client_factory=client_factory)
    assert servicer.get() == health_pb2.HealthCheckResponse.ServingStatus.SERVING


@patch("kaskada.health.health_check_client.HealthCheckClientFactory")
@patch("kaskada.health.health_check_client.HealthCheckClient")
def test_health_check_servicer_get_default(client_factory, client):
    """Tests the get method uses the latest checked health status by adding an unhealthy client.
    Prior to checking the health, the status should be SERVING.
    Checking will return UNKNOWN as a mock.
    After the status should be UNKNOWN
    """
    servicer = HealthCheckServicer(client_factory=client_factory)
    client_factory.get_client = MagicMock(return_value=client)
    client.check = MagicMock(
        return_value=health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
    )
    servicer = HealthCheckServicer(client_factory=client_factory)
    servicer.add_service("test_service", "test_endpoint", True)
    assert servicer.get() == health_pb2.HealthCheckResponse.ServingStatus.SERVING
    servicer.check()
    assert servicer.get() == health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
