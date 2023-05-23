import logging
import subprocess
from pathlib import Path
from subprocess import Popen
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class SubprocessFactory:
    def __init__(self):
        pass

    def get_subprocess(
        self, cmd: List[str], stderr: Path, stdout: Path
    ) -> subprocess.Popen:
        return subprocess.Popen(
            cmd,
            stderr=open(stderr, "w", encoding="utf-8"),
            stdout=open(stdout, "w", encoding="utf-8"),
        )


class KaskadaLocalService:
    """Represents the resources associated with running a local service. This includes:
    - Service name
    - Path to the binary with execution command
    - Path to std err
    - Path to std out
    - Configurations to run the binary
    """

    process: Optional[Popen] = None

    def __init__(
        self,
        service_name: str,
        binary_path: str,
        binary_execute_cmd: str,
        std_err_log_path: Path,
        std_out_log_path: Path,
        configs: Dict[str, Any],
        subprocess_factory=SubprocessFactory(),
    ):
        """Instantiates a new Kaskada Local Service

        Args:
            service_name (str): the name of the service
            binary_path (str): the path to the binary with any arguments
            binary_execute_cmd (List[str]): the binary execution arguments
            std_err_log_path (Path): the path to log STD ERR
            std_out_log_path (Path): the path to log STD OUT
            configs (Dict[str, Any]): the configurations to pass to the binary execution
        """
        self.service_name = service_name
        self.binary_path = binary_path
        self.binary_execute_cmd = binary_execute_cmd
        self.std_err_log_path = std_err_log_path
        self.std_out_log_path = std_out_log_path
        self.configs = configs
        self.subprocess_factory = subprocess_factory
        self.execute_cmd = self.__get_subprocess_cmd()

    def start(self):
        """Starts the local service."""
        logger.debug(f"{self.service_name} start command: {self.execute_cmd}")
        logger.info(f"Initializing {self.service_name} process")
        logger.info(
            f"Logging {self.service_name} STDOUT to {self.std_out_log_path.absolute()}"
        )
        logger.info(
            f"Logging {self.service_name} STDERR to {self.std_out_log_path.absolute()}"
        )
        self.process = self.subprocess_factory.get_subprocess(
            self.execute_cmd, self.std_err_log_path, self.std_out_log_path
        )

    def is_running(self) -> bool:
        """Reports if a local service is running by checking the return code of the process.

        Returns:
            bool: True if the polled process has not returned. False otherwise.
        """
        if self.process is None:
            return False
        poll_return_code = self.process.poll()
        if poll_return_code is None:
            return True
        return False

    def stop(self, max_wait_seconds: int = 5):
        """Stops the local service gracefully by sending a SIGTERM."""
        if self.process is not None:
            self.process.terminate()
            self.process.wait(max_wait_seconds)

    def __get_configs_as_args(self):
        configs = []
        for key, value in self.configs.items():
            configs.append(f"{key}={value}")
        return configs

    def __get_subprocess_cmd(self):
        return (
            [self.binary_path]
            + self.__get_configs_as_args()
            + [self.binary_execute_cmd]
        )
