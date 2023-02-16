import os
import time
import uuid
from abc import ABC
from pathlib import Path
from subprocess import Popen
from typing import Optional

import kaskada.client
from kaskada.api import api_utils, release


class Session:
    def __init__(
        self,
        endpoint: str,
        is_secure: bool,
        name: str,
        client_id: Optional[str] = None,
        api_process: Optional[Popen] = None,
        compute_process: Optional[Popen] = None,
    ) -> None:
        self._endpoint = endpoint
        self._is_secure = is_secure
        self._name = name
        self._client_id = client_id
        self._client = self.connect()
        self._api_process = api_process
        self._compute_process = compute_process

    def __del__(self):
        if self._api_process is not None:
            print("Stopping API server")
            self._api_process.kill()

        if self._compute_process is not None:
            print("Stopping Compute server")
            self._compute_process.kill()

    def connect(self):
        attempt = 0
        is_valid_session = False
        while attempt < 3 and not is_valid_session:
            # Sleep with exponential backoff
            time.sleep(1.5**attempt)
            if api_utils.check_socket(self._endpoint):
                is_valid_session = True
            attempt += 1
        if is_valid_session == False:
            raise ConnectionError(
                "unable to connect to API or Compute engine after {} attempts".format(
                    attempt
                )
            )
        return kaskada.client.init(self._client_id, self._endpoint, self._is_secure)


class Builder(ABC):
    def __init__(
        self,
        endpoint: Optional[str] = None,
        is_secure: Optional[bool] = None,
        name: str = str(uuid.uuid4()),
        client_id: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._endpoint: Optional[str] = endpoint
        self._is_secure: Optional[bool] = is_secure
        self._name: str = name
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
        self._download = True

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

    def __get_log_path(self, file_name: str) -> Path:
        if self._path is None:
            raise ValueError("no path provided and KASKADA_PATH was not set")
        if self._log_path is None:
            raise ValueError("no log path provided and KASKADA_LOG_PATH was not set")
        log_path = Path(
            "{}/{}/{}".format(self._path, self._log_path, self._name)
        ).expanduser()
        log_path.mkdir(parents=True, exist_ok=True)
        return log_path / file_name

    def __get_binary_path(self) -> Path:
        if self._path is None:
            raise ValueError("no path provided and KASKADA_PATH was not set")
        if self._bin_path is None:
            raise ValueError("no bin path provided and KASKADA_BIN_PATH was not set")
        bin_path = Path("{}/{}".format(self._path, self._bin_path)).expanduser()
        bin_path.mkdir(parents=True, exist_ok=True)
        return bin_path

    def __start(self):
        api_binary_path = (
            self.__get_binary_path() / self.KASKADA_MANAGER_BIN_NAME_DEFAULT
        )
        api_log_path = self.__get_log_path("api_logs.txt")
        compute_binary_path = (
            self.__get_binary_path() / self.KASKADA_ENGINE_BIN_NAME_DEFAULT
        )
        compute_log_path = self.__get_log_path("compute_logs.txt")
        compute_params = "serve"

        # TODO: Verify the logging output (stdout/stderr)
        api_cmd = "{} > {} 2>&1".format(api_binary_path, api_log_path)
        compute_cmd = "{} {} > {} 2>&1".format(
            compute_binary_path, compute_params, compute_log_path
        )
        api_process = api_utils.run_subprocess(api_cmd)
        compute_process = api_utils.run_subprocess(compute_cmd)
        return (api_process, compute_process)

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
        api_process, compute_process = self.__start()
        if self._endpoint is None:
            raise ValueError("endpoint was not set")
        if self._is_secure is None:
            raise ValueError("is_secure was not set")

        return Session(
            self._endpoint,
            self._is_secure,
            self._name,
            client_id=self._client_id,
            api_process=api_process,
            compute_process=compute_process,
        )


class RemoteBuilder(Builder):
    def __init__(self) -> None:
        super().__init__()

    def build(self):
        return Session(
            self._endpoint, self._is_secure, self._name, client_id=self._client_id
        )
