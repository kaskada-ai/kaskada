import logging
import os
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import kaskada.client
from kaskada.api import release
from kaskada.api.local_session.local_service import KaskadaLocalService
from kaskada.api.local_session.local_session_keep_alive import LocalSessionKeepAlive

logger = logging.getLogger(__name__)


KASKADA_PATH_ENV = "KASKADA_PATH"
KASKADA_PATH_DEFAULT = "~/.cache/kaskada"
KASKADA_LOG_PATH_DEFAULT = "logs"
KASKADA_LOG_PATH_ENV = "KASKADA_LOG_PATH"
KASKADA_BIN_PATH_DEFAULT = "bin"
KASKADA_BIN_PATH_ENV = "KASKADA_BIN_PATH"

KASKADA_MANAGER_BIN_NAME_DEFAULT = "kaskada-manager"
KASKADA_ENGINE_BIN_NAME_DEFAULT = "kaskada-engine"

KASKADA_DISABLE_DOWNLOAD_ENV = "KASKADA_DISABLE_DOWNLOAD"


class Session:
    def __init__(
        self,
        endpoint: str,
        is_secure: bool,
        client_id: Optional[str] = None,
        client_factory: Optional[kaskada.client.ClientFactory] = None,
    ) -> None:
        self._endpoint = endpoint
        self._is_secure = is_secure
        self._client_id = client_id
        self.client: Optional[kaskada.client.Client] = None
        if client_factory is None:
            self.client_factory = kaskada.client.ClientFactory(
                self._client_id, self._endpoint, self._is_secure
            )
        else:
            self.client_factory = client_factory

    def stop(self):
        kaskada.client.reset()
        global KASKADA_SESSION
        KASKADA_SESSION = None

    def connect(self, should_check_health: bool = True) -> kaskada.client.Client:
        self.client = self.client_factory.get_client(
            should_check_health=should_check_health
        )
        assert self.client is not None
        kaskada.client.set_default_client(self.client)
        return self.client


class LocalSession(Session):
    """An extension of a kaskada.api.Session that is executed locally. The local session is kept alive by using the LocalSessionKeepAlive."""

    keep_alive_watcher: Optional[LocalSessionKeepAlive] = None

    def __init__(
        self,
        endpoint: str,
        is_secure: bool,
        manager_service: KaskadaLocalService,
        engine_service: KaskadaLocalService,
        client_id: Optional[str] = None,
        should_keep_alive: bool = True,
    ):
        """Instantiates a LocalSession.

        Args:
            endpoint (str): The endpoint of the manager service
            is_secure (bool): True to use TLS or False to use an insecure connection
            manager_service (KaskadaLocalService): The manager service resource
            engine_service (KaskadaLocalService): The engine service resource
            client_id (Optional[str], optional): A custom client ID. Defaults to None.
        """
        super().__init__(endpoint, is_secure, client_id)
        self.manager_service = manager_service
        self.engine_service = engine_service
        self.should_keep_alive = should_keep_alive

    def start(self):
        """Starts the local session by calling start on all services and connect on the client."""
        self.manager_service.start()
        self.engine_service.start()
        client = self.connect()
        self.keep_alive_watcher = LocalSessionKeepAlive(
            self.manager_service,
            self.engine_service,
            client,
            should_check=self.should_keep_alive,
        )
        self.keep_alive_watcher.start()

    def stop(self):
        """Stops the local session by calling stop on all services and stops the keep alive watcher."""
        self.keep_alive_watcher.stop()
        self.keep_alive_watcher.join()
        self.client.disconnect()
        super().stop()
        self.manager_service.stop()
        self.engine_service.stop()
        logger.info("Local session successfully stopped.")


KASKADA_SESSION: Optional[LocalSession] = None


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


class LocalBuilder(Builder):

    manager_configs: Dict[str, Any]
    engine_configs: Dict[str, Any]

    def __init__(
        self,
        endpoint: str = kaskada.client.KASKADA_DEFAULT_ENDPOINT,
        is_secure: bool = kaskada.client.KASKADA_IS_SECURE,
    ) -> None:
        super().__init__()
        self.manager_configs = {"-no-color": "1"}
        self._path: str = os.getenv(KASKADA_PATH_ENV, KASKADA_PATH_DEFAULT)
        self._bin_path: str = os.getenv(KASKADA_BIN_PATH_ENV, KASKADA_BIN_PATH_DEFAULT)
        self._log_path: str = os.getenv(KASKADA_LOG_PATH_ENV, KASKADA_LOG_PATH_DEFAULT)
        self.endpoint(endpoint, is_secure)
        self._download = os.getenv(KASKADA_DISABLE_DOWNLOAD_ENV, "false") != "true"
        self.in_memory(False)
        self.engine_configs: Dict[str, Any] = {"--log-no-color": "1"}
        self.keep_alive(True)

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

    def keep_alive(self, keep_alive: bool):
        self._keep_alive = keep_alive
        return self

    def in_memory(self, in_memory: bool):
        self.manager_configs["-db-in-memory"] = "1" if in_memory else "0"
        return self

    def database_path(self, path: str):
        self.manager_configs["-db-path"] = path
        self.in_memory(False)
        return self

    def with_manager_args(self, configs: List[Tuple[str, Any]]):
        """Configure the Manager to run with a list of arguments. The arguments must be prefixed with a "-" and boolean values are represented as "1" or "0".

        For example:
        ```
        from kaskada.api.session import LocalBuilder
            session = LocalBuilder().with_manager_configs([
                ("-object-store-type", "local"),
                ("-object-store-path", "/Users/kevin.nguyen/Github/kaskada/examples3"),
                ("-db-in-memory", "1"),
                ("-rest-port", 12345)
            ]).build()
        ```

        Args:
            configs (List[Tuple[str, Any]]): Manager arguments
        """
        for config in configs:
            self.manager_configs[config[0]] = config[1]
        return self

    def manager_rest_port(self, port: int):
        self.manager_configs["-rest-port"] = port
        return self

    def manager_grpc_port(self, port: int):
        self.manager_configs["-grpc-port"] = port
        return self

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

    def __get_local_services(self) -> Tuple[KaskadaLocalService, KaskadaLocalService]:
        manager_binary_path = (
            self.__get_binary_path() / KASKADA_MANAGER_BIN_NAME_DEFAULT
        )
        manager_std_err, manager_std_out = self.__get_std_paths("manager")
        engine_binary_path = self.__get_binary_path() / KASKADA_ENGINE_BIN_NAME_DEFAULT
        engine_std_err, engine_std_out = self.__get_std_paths("engine")
        engine_command = "serve"
        manager_service = KaskadaLocalService(
            "manager",
            str(manager_binary_path),
            "",
            manager_std_err,
            manager_std_out,
            self.manager_configs,
        )
        engine_service = KaskadaLocalService(
            "engine",
            str(engine_binary_path),
            engine_command,
            engine_std_err,
            engine_std_out,
            self.engine_configs,
        )
        return (manager_service, engine_service)

    def __download_latest_release(self):
        """Downloads the latest release version to the binary path."""
        client = release.ReleaseClient()
        download_path = self.__get_binary_path()
        download_path.mkdir(parents=True, exist_ok=True)
        local_release = client.download_latest_release(
            download_path,
            KASKADA_MANAGER_BIN_NAME_DEFAULT,
            KASKADA_ENGINE_BIN_NAME_DEFAULT,
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

    def build(self) -> LocalSession:
        """Builds the local session. Starts by downloading the latest release and starting the local binaries.

        Returns:
            LocalSession: The local session object
        """
        if self._endpoint is None:
            raise ValueError("endpoint was not set")
        if self._is_secure is None:
            raise ValueError("is_secure was not set")

        global KASKADA_SESSION
        if KASKADA_SESSION is not None and KASKADA_SESSION.client is not None:
            logger.info("Detected an existing session.")
            is_ready = KASKADA_SESSION.client.is_ready()
            if is_ready:
                logger.info(
                    "Existing session is available and ready to accept requests. Reusing existing session."
                )
                return KASKADA_SESSION
            else:
                logger.warn(
                    "Existing session is no longer available. Creating a new session."
                )

        if self._download:
            self.__download_latest_release()

        manager_process, engine_process = self.__get_local_services()
        session = LocalSession(
            self._endpoint,
            self._is_secure,
            manager_process,
            engine_process,
            client_id=self._client_id,
        )
        session.start()
        KASKADA_SESSION = session
        return session
