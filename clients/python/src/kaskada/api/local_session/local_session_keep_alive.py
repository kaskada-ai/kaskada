import logging
import threading
import time

from kaskada.api.local_session.local_service import KaskadaLocalService
from kaskada.client import Client

logger = logging.getLogger(__name__)


class LocalSessionKeepAlive(threading.Thread):
    """An implementation of a thread to poll the Local Session for the overall status and recover from failure by restarting the services.
    This implementation is thread-safe.
    """

    def __init__(
        self,
        manager_service: KaskadaLocalService,
        engine_service: KaskadaLocalService,
        client: Client,
        interval_seconds: int = 1,
        maximum_unready_polls: int = 3,
        should_check: bool = True,
    ):
        """Instantiates a LocalSessionKeepAlive.

        Args:
            manager_service (KaskadaLocalService): The manager service resource
            engine_service (KaskadaLocalService): The engine service resource
            client (Client): The client used to connect to the manager
            interval_seconds (int, optional): Interval to wait between polls (seconds). Defaults to 1 second.
            maximum_unready_polls (int, optional): Maximum number of unready polls before attempting to recover. Defaults to 3.
        """
        threading.Thread.__init__(self)
        self.manager_service = manager_service
        self.engine_service = engine_service
        self.client = client
        self.lock = threading.Lock()
        self.should_check = should_check
        self.interval_seconds = interval_seconds
        self.maximum_unready_polls = maximum_unready_polls

    def run(self):
        """Runs indefinitely and monitors the status of the manager service, the engine service, and the connected client.
        If any of them report unready, then all of the services are restarted and the client reconnects.

        To stop the keep alive, use the stop() method to trigger the thread to conclude.
        """
        unready_count = 0
        while True:
            self.lock.acquire()
            try:
                if not self.should_check:
                    logger.debug(
                        "detected stop in keep alive. no longer monitoring local session."
                    )
                    return
            finally:
                self.lock.release()

            if (
                self.manager_service.is_running()
                and self.engine_service.is_running()
                and self.client.is_ready()
            ):
                unready_count = 0
            else:
                unready_count += 1
                logger.warn(
                    f"Kaskada services are not available (Attempt: #{unready_count})"
                )

            if unready_count >= self.maximum_unready_polls:
                logger.info(f"Attempting to recover from an invalid session.")
                self.manager_service.stop()
                self.engine_service.stop()
                unready_count = 0
                self.manager_service.start()
                self.engine_service.start()
                self.client.connect()

            time.sleep(self.interval_seconds)

    def stop(self):
        """Triggers the keep alive to stop checking and conclude."""
        self.lock.acquire()
        self.should_check = False
        self.lock.release()
