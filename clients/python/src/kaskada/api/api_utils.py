import platform
import socket
import subprocess
from contextlib import closing


def run_subprocess(cmd: str):
    return subprocess.Popen(cmd, shell=True)


def check_socket(endpoint: str) -> bool:
    parse = endpoint.split(":")
    if len(parse) != 2:
        raise ValueError("endpoint is not formatted correctly. host:port")

    host = parse[0]
    port = int(parse[1])
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


def get_platform_details() -> str:
    """Gets the platform string mapping

    Returns:
        str: The current system platform mapping
    """
    platform_str = f"{platform.system().lower()}-{platform.machine()}"
    PLATFORMS = {
        "linux-x86_64": "linux-amd64",
        "darwin-x86_64": "darwin-amd64",
        "darwin-arm64": "darwin-arm64",
        "windows-amd64": "windows-amd64",
    }

    try:
        return PLATFORMS[platform_str]
    except KeyError:
        raise RuntimeError(f"unable to find binary matching: {platform_str}")
