import platform


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
