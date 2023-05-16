import logging
import os
import sys
import time
from abc import ABC
from datetime import datetime
from pathlib import Path
from subprocess import Popen
from typing import Any, Dict, Optional, Tuple

import kaskada.client
from kaskada.api import api_utils, release

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class Session:
    def __init__(
        self,
        endpoint: str,
        is_secure: bool,
        client_id: Optional[str] = None,
        manager_process: Optional[Popen] = None,
        engine_process: Optional[Popen] = None,
    ) -> None:
        self._endpoint = endpoint
        self._is_secure = is_secure
        self._client_id = client_id
        self._client = self.connect()
        self._manager_process = manager_process
        self._engine_process = engine_process

    def __del__(self):
        if self._manager_process is not None:
            logger.info(
                "Stopping Kaskada Manager service"
            ) if logger is not None else None
            self._manager_process.kill()

        if self._engine_process is not None:
            logger.info(
                "Stopping Kaskada Engine service"
            ) if logger is not None else None
            self._engine_process.kill()

    def connect(self):
        attempt = 0
        is_valid_session = False
        while attempt < 3 and not is_valid_session:
            logger.debug(f"Attempting (try #{attempt}) to connect to {self._endpoint}")
            if api_utils.check_socket(self._endpoint):
                logger.info(f"Successfully connected to session.")
                is_valid_session = True
            attempt += 1
            # Sleep with exponential backoff
            time.sleep(1.5**attempt)
        if is_valid_session == False:
            raise ConnectionError(
                "unable to connect to Manager or Engine after {} attempts".format(
                    attempt
                )
            )
        return kaskada.client.init(self._client_id, self._endpoint, self._is_secure)


class Builder(ABC):
    def __init__(
        self,
        endpoint: Optional[str] = None,
        is_secure: Optional[bool] = None,
        name: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._endpoint: Optional[str] = endpoint
        self._is_secure: Optional[bool] = is_secure
        self._name: Optional[str] = name
        self._client_id: Optional[str] = client_id

    def endpoint(self, endpoint: str, is_secure: bool):
        self._endpoint = endpoint
        self._is_secure = is_secure
        return self

    def name(self, name: str):
        self._name = name
        return self

    def client_id(self, client_id: str):
        self._client_id = client_id
        return self


KASKADA_ENDPOINT_DEFAULT = "localhost:50051"
KASKADA_IS_SECURE_DEFAULT = False


class LocalBuilder(Builder):
    KASKADA_PATH_ENV = "KASKADA_PATH"
    KASKADA_PATH_DEFAULT = "~/.cache/kaskada"
    KASKADA_LOG_PATH_DEFAULT = "logs"
    KASKADA_LOG_PATH_ENV = "KASKADA_LOG_PATH"
    KASKADA_BIN_PATH_DEFAULT = "bin"
    KASKADA_BIN_PATH_ENV = "KASKADA_BIN_PATH"

    KASKADA_MANAGER_BIN_NAME_DEFAULT = "kaskada-manager"
    KASKADA_ENGINE_BIN_NAME_DEFAULT = "kaskada-engine"

    KASKADA_DISABLE_DOWNLOAD_ENV = "KASKADA_DISABLE_DOWNLOAD"

    def __init__(
        self,
        endpoint: str = KASKADA_ENDPOINT_DEFAULT,
        is_secure: bool = KASKADA_IS_SECURE_DEFAULT,
    ) -> None:
        super().__init__()
        self._path: str = os.getenv(
            LocalBuilder.KASKADA_PATH_ENV, LocalBuilder.KASKADA_PATH_DEFAULT
        )
        self._bin_path: str = os.getenv(
            LocalBuilder.KASKADA_BIN_PATH_ENV, LocalBuilder.KASKADA_BIN_PATH_DEFAULT
        )
        self._log_path: str = os.getenv(
            LocalBuilder.KASKADA_LOG_PATH_ENV, LocalBuilder.KASKADA_LOG_PATH_DEFAULT
        )
        self.endpoint(endpoint, is_secure)
        self._download = (
            os.getenv(LocalBuilder.KASKADA_DISABLE_DOWNLOAD_ENV, "false") != "true"
        )
        self._manager_configs: Dict[str, Any] = {"-no-color": "1"}
        self._engine_configs: Dict[str, Any] = {"--log-no-color": "1"}

    def path(self, path: str):
        self._path = path
        return self

    def log_path(self, path: str):
        self._log_path = path
        return self

    def bin_path(self, path: str):
        self._bin_path = path
        return self

    def download(self, download: bool):
        self._download = download
        return self

    def manager_rest_port(self, port: int):
        self._manager_configs["-rest-port"] = port
        return self

    def manager_grpc_port(self, port: int):
        self._manager_configs["-grpc-port"] = port
        return self

    def __get_manager_configs_as_args(self):
        configs = []
        for key, value in self._manager_configs.items():
            configs.append(f"{key}={value}")
        return configs

    def __get_engine_configs_as_args(self):
        configs = []
        for key, value in self._engine_configs.items():
            configs.append(f"{key}={value}")
        return configs

    def __get_log_path(self, file_name: str) -> Path:
        if self._path is None:
            raise ValueError("no path provided and KASKADA_PATH was not set")
        if self._log_path is None:
            raise ValueError("no log path provided and KASKADA_LOG_PATH was not set")
        log_path = Path("{}/{}".format(self._path, self._log_path)).expanduser()
        if self._name is not None:
            log_path = log_path / self._name
        log_path.mkdir(parents=True, exist_ok=True)
        return log_path / file_name

    def __get_std_paths(self, service_name: str) -> Tuple[Path, Path]:
        current_time = datetime.now()
        timestamp_format = current_time.strftime("%Y-%m-%dT%H-%M-%S")
        stderr = self.__get_log_path(f"{timestamp_format}-{service_name}-stderr.log")
        stdout = self.__get_log_path(f"{timestamp_format}-{service_name}-stdout.log")
        return (stderr, stdout)

    def __get_binary_path(self) -> Path:
        if self._path is None:
            raise ValueError("no path provided and KASKADA_PATH was not set")
        if self._bin_path is None:
            raise ValueError("no bin path provided and KASKADA_BIN_PATH was not set")
        bin_path = Path("{}/{}".format(self._path, self._bin_path)).expanduser()
        bin_path.mkdir(parents=True, exist_ok=True)
        return bin_path

    def __start(self):
        manager_binary_path = (
            self.__get_binary_path() / self.KASKADA_MANAGER_BIN_NAME_DEFAULT
        )
        manager_std_err, manager_std_out = self.__get_std_paths("manager")
        engine_binary_path = (
            self.__get_binary_path() / self.KASKADA_ENGINE_BIN_NAME_DEFAULT
        )
        engine_std_err, engine_std_out = self.__get_std_paths("engine")
        engine_command = "serve"

        manager_cmd = [str(manager_binary_path)] + self.__get_manager_configs_as_args()
        logger.debug(f"Manager start command: {manager_cmd}")
        engine_cmd = (
            [str(engine_binary_path)]
            + self.__get_engine_configs_as_args()
            + [str(engine_command)]
        )
        logger.debug(f"Engine start command: {engine_cmd}")
        logger.info("Initializing manager process")
        logger.info(f"Logging manager STDOUT to {manager_std_out.absolute()}")
        logger.info(f"Logging manager STDERR to {manager_std_err.absolute()}")
        manager_process = api_utils.run_subprocess(
            manager_cmd, manager_std_err, manager_std_out
        )
        logger.info("Initializing engine process")
        logger.info(f"Logging engine STDOUT to {engine_std_out.absolute()}")
        logger.info(f"Logging engine STDERR to {engine_std_err.absolute()}")
        engine_process = api_utils.run_subprocess(
            engine_cmd, engine_std_err, engine_std_out
        )
        return (manager_process, engine_process)

    def __download_latest_release(self):
        """Downloads the latest release version to the binary path."""
        client = release.ReleaseClient()
        download_path = self.__get_binary_path()
        download_path.mkdir(parents=True, exist_ok=True)
        local_release = client.download_latest_release(
            download_path,
            self.KASKADA_MANAGER_BIN_NAME_DEFAULT,
            self.KASKADA_ENGINE_BIN_NAME_DEFAULT,
        )
        logger.debug(f"Download Path: {local_release._download_path}")
        logger.debug(f"Manager Path: {local_release._manager_path}")
        logger.debug(f"Engine Path: {local_release._engine_path}")
        # Update the binary path to the path downloaded and saved to by the latest release downloader.
        self.bin_path(
            local_release._download_path.absolute().relative_to(
                Path(self._path).expanduser().absolute()
            )
        )
        os.chmod(local_release._manager_path, 0o755)
        os.chmod(local_release._engine_path, 0o755)

    def build(self) -> Session:
        """Builds the local session. Starts by downloading the latest release and starting the local binaries.

        Returns:
            Session: The local session object
        """
        if self._download:
            self.__download_latest_release()
        manager_process, engine_process = self.__start()
        if self._endpoint is None:
            raise ValueError("endpoint was not set")
        if self._is_secure is None:
            raise ValueError("is_secure was not set")

        return Session(
            self._endpoint,
            self._is_secure,
            client_id=self._client_id,
            manager_process=manager_process,
            engine_process=engine_process,
        )


class RemoteBuilder(Builder):
    def __init__(self) -> None:
        super().__init__()

    def build(self):
        return Session(self._endpoint, self._is_secure, client_id=self._client_id)
