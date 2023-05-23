import logging
import threading
import time

from grpc.health.v1 import health_pb2, health_pb2_grpc

from kaskada.health.health_check_servicer import HealthCheckServicer

logger = logging.getLogger(__name__)


class HealthCheckWatcher(threading.Thread):
    """An implementation of a thread to poll the HealthCheckServicer for the overall status. This implementation is thread-safe."""

    def __init__(self, health_servicer: HealthCheckServicer, interval_seconds: int = 1):
        threading.Thread.__init__(self)
        self.health_servicer = health_servicer
        self.health_check_lock = threading.Lock()
        self.enabled = True
        self.status = health_pb2.HealthCheckResponse.ServingStatus.SERVING
        self.interval_seconds = interval_seconds

    def run(self):
        """Starts the thread to continuously poll the provided HealthCheckServicer for the health of the services.
        To stop the watcher, use the thread-safe stop() method.
        This thread is designed to infinitely run until the stop() method is called.
        """
        while True:
            self.health_check_lock.acquire()
            enabled = self.enabled
            self.health_check_lock.release()
            if enabled:
                status = self.health_servicer.check()
                self.health_check_lock.acquire()
                self.status = status
                self.health_check_lock.release()
                time.sleep(self.interval_seconds)
            else:
                break

    def stop(self):
        """Stops the HealthCheckWatcher. This method acquires a mutex lock to signal the thread to stop and therefore the thread is not stopped instantly."""
        self.health_check_lock.acquire()
        self.enabled = False
        self.health_check_lock.release()

    def get_status(self) -> health_pb2.HealthCheckResponse.ServingStatus:
        """Gets the status polled from the watcher. This method does not fetch the latest status but reads the latest fetched value between intervals.

        Returns:
            health_pb2.HealthCheckResponse.ServingStatus: the latest aggregated health status from the HealthCheckServicer.
        """
        self.health_check_lock.acquire()
        try:
            return self.status
        finally:
            self.health_check_lock.release()
