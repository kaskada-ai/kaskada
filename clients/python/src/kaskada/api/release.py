import functools
import logging
import os
import shutil
import sys
from pathlib import Path

import requests
from github import Github

import kaskada.api.api_utils

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class LocalRelease(object):
    """Configuration details for a local release. Mostly used for binary paths."""

    def __init__(
        self, download_path: Path, manager_path: Path, engine_path: Path
    ) -> None:
        self._download_path = download_path
        self._manager_path = manager_path
        self._engine_path = engine_path


class ReleaseClient(object):
    ORGANIZATION = "kaskada-ai"
    REPO_NAME = "kaskada"
    # In case the repository is private, an authorized access token is required.
    GITHUB_ACCESS_TOKEN_ENV = "GITHUB_ACCESS_TOKEN"
    ASSETS_API_ENDPOINT = "https://api.github.com/repos/{}/{}/releases/assets".format(
        ORGANIZATION, REPO_NAME
    )

    def __init__(self) -> None:
        super().__init__()
        self._platform_details = kaskada.api.api_utils.get_platform_details()
        # Private repository access
        access_token = os.getenv(self.GITHUB_ACCESS_TOKEN_ENV)
        self._github = Github(access_token)

    def download_latest_release(
        self, download_path: Path, manager_bin_name: str, engine_bin_name: str
    ) -> LocalRelease:
        """Downloads the latest version of the kaskada-manager and kaskada-engine services.

        Args:
            download_path (Path): The local download path
            manager_bin_name (str): The name of the manager binary to save in download path
            engine_bin_name (str): The name of the engine binary to save in download path

        Raises:
            RuntimeError: unable to get release assets

        Returns:
            Tuple[str, str]: manager path and engine path
        """
        manager_name = f"kaskada-manager-{self._platform_details}"
        logger.debug(f"Looking for manager binary name: {manager_name}")
        engine_name = f"kaskada-engine-{self._platform_details}"
        logger.debug(f"Looking for engine binary name: {engine_name}")
        session = requests.Session()
        access_token = os.getenv(self.GITHUB_ACCESS_TOKEN_ENV)
        if access_token is not None:
            logger.debug("Access token set. Using Github Access token.")
            session.headers["Authorization"] = f"token {access_token}"

        repo = self._github.get_repo(f"{self.ORGANIZATION}/{self.REPO_NAME}")
        latest_release = repo.get_latest_release()
        logger.info(f"Using latest release version: {latest_release.tag_name}")
        download_path = download_path / latest_release.tag_name
        download_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Download path: {download_path}")
        assets = latest_release.get_assets()

        manager_path, engine_path = (None, None)
        for asset in assets:
            url = f"{self.ASSETS_API_ENDPOINT}/{asset.id}"
            if asset.name == manager_name:
                manager_path = self.__download(
                    session,
                    url,
                    download_path / manager_bin_name,
                    asset.name,
                    file_size=asset.size,
                )
            elif asset.name == engine_name:
                engine_path = self.__download(
                    session,
                    url,
                    download_path / engine_bin_name,
                    asset.name,
                    file_size=asset.size,
                )

        if manager_path is None:
            raise RuntimeError(f"unable to download manager binary: {manager_name}")
        if engine_path is None:
            raise RuntimeError(f"unable to download engine binary: {engine_name}")
        return LocalRelease(download_path, manager_path, engine_path)

    def __download(
        self,
        r: requests.Session,
        url: str,
        download_path: Path,
        description: str,
        file_size: int = 0,
    ) -> Path:
        """Downloads a URL as an application/octet-stream

        Args:
            r (requests.Session): The request session
            url (str): The targget URL
            download_path (Path): The local path to stream write the file
            description (str): The description to render during download
            file_size (int, optional): The file size if known ahead of time. Defaults to response content size or 0.

        Raises:
            RuntimeError: Unable to download the target URL due to non 200 error code.

        Returns:
            Path: The local download path
        """
        logger.debug(
            f"Downloading: {url} to {download_path}. Content Size: {file_size}"
        )
        # A naive cache hit algorithm where a hit is if the file exists and is the same size as the asset.
        if download_path.exists() and download_path.stat().st_size == file_size:
            logger.info(f"Skipping download. Using binary: {download_path}")
            return download_path
        # Request header to get the binary
        r.headers["Accept"] = "application/octet-stream"
        response = r.get(url, stream=True, allow_redirects=True)
        if response.status_code != 200:
            response.raise_for_status()  # If there was an HTTP error, raise that first otherwise generic error.
            raise RuntimeError(
                f"request to download binary failed with status code: {response.status_code}"
            )
        if (
            file_size == 0
        ):  # The file size is only used for the progress bar visualization.
            file_size = int(r.headers.get("Content-Length", 0))
        desc = "(Unknown total file size)" if file_size == 0 else description
        response.raw.read = functools.partial(
            response.raw.read, decode_content=True
        )  # Decompress if needed
        from tqdm.auto import tqdm

        with tqdm.wrapattr(
            response.raw, "read", total=file_size, desc=desc
        ) as r_raw:  # Define the progress bar
            with download_path.open(
                "wb"
            ) as f:  # Start downloading the raw response as the binary
                shutil.copyfileobj(r_raw, f)
        logger.debug(f"Successfully downloaded to: {download_path}")
        return download_path
