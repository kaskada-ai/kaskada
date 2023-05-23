import time
from unittest.mock import MagicMock, call, patch

import pytest
from grpc.health.v1 import health_pb2

from kaskada.health.health_check_watcher import HealthCheckWatcher


@patch("kaskada.health.health_check_servicer.HealthCheckServicer")
def test_health_check_watcher(servicer):
    servicer.check.side_effect = [
        health_pb2.HealthCheckResponse.ServingStatus.SERVING,
        health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN,
    ]
    watcher = HealthCheckWatcher(servicer, interval_seconds=0.5)
    # Without calling start, the default should be SERVING since no health check has been polled
    assert watcher.get_status() == health_pb2.HealthCheckResponse.ServingStatus.SERVING
    watcher.start()
    # Calling time.sleep to allow the watcher to poll once to return SERVING
    time.sleep(0.1)
    assert watcher.get_status() == health_pb2.HealthCheckResponse.ServingStatus.SERVING
    # Calling time.sleep to allow one iteration (interval_seconds = 0.5), the next call should be UNKNOWN
    time.sleep(0.6)
    assert watcher.get_status() == health_pb2.HealthCheckResponse.ServingStatus.UNKNOWN
    watcher.stop()
