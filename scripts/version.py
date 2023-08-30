import tomlkit
import argparse
from typing import Dict
from typing import List
from packaging.version import parse

def get_value_from_toml(file_path: str, toml_path: str | List[str]) -> str:
    """Retrieve a value from a TOML file at the given path."""
    with open(file_path, 'r') as f:
        data = tomlkit.parse(f.read())
    if isinstance(toml_path, str):
        toml_path = toml_path.split('.')
    temp = data
    for key in toml_path:
        temp = temp[key]

    return str(temp)  # Convert value to string in case it's a number or boolean


def update_version_in_data(data: Dict, version: str, toml_paths: List[str]) -> None:
    """Update the version number in a data dictionary (parsed TOML) at multiple paths."""
    for path in toml_paths:
        temp = data
        path = path.split('.')
        for key in path[:-1]:
            temp = temp[key]
        temp[path[-1]] = version


def update_versions(entries: List[str], version: str) -> None:
    """Update the version number in the given entries."""
        # Dictionary to hold the paths for each file
    file_paths_dict = defaultdict(list)

    for entry in args.entries:
        parts = entry.split(":")
        if len(parts) != 2:
            print(f"Invalid entry format: {entry}")
            continue

        file_path, toml_path_str = parts

        file_paths_dict[file_path].append(toml_path_str)

    # Update the files using the stored paths
    for file_path, paths in file_paths_dict.items():
        with open(file_path, 'r') as f:
            data = tomlkit.parse(f.read())

        update_version_in_data(data, args.version, paths)

        with open(file_path, 'w') as f:
            f.write(dumps(data))

def normalize_version(version_str: str) -> str:
    """Normalize a version string."""
    version = parse(version_str)
    return str(version)

def main() -> None:
    parser = argparse.ArgumentParser(description="Manage versions in TOML files")
    subparsers = parser.add_subparsers(dest="command")

    # Setup 'get' command
    get_parser = subparsers.add_parser("get", help="Get version from a toml_file at a specific path")
    get_parser.add_argument("toml_file", type=str, help="Path to the TOML file")
    get_parser.add_argument("path", type=str, help="Path within the TOML file (e.g., package.version)")
    get_parser.add_argument("--normalize", action="store_true", help="Normalize the version")

    # Setup 'set' command
    set_parser = subparsers.add_parser("set", help="Set version in one or more TOML files")
    set_parser.add_argument("version", type=str, help="Version to set")
    set_parser.add_argument("entries", nargs='+', type=str,
        help="TOML file and path, format: <file_path>:<toml_path> (e.g., config.toml:package.version)")

    # Setup 'normalize' command
    normalize_parser = subparsers.add_parser("normalize", help="Normalize version")
    normalize_parser.add_argument("version", type=str, help="Version to normalize")

    args = parser.parse_args()
    if args.command == "get":
        version = get_value_from_toml(args.toml_file, args.path)
        if args.normalize:
            version = normalize_version(version)
        print(version)
    elif args.command == "set":
        set_version(args.toml_file, args.key, args.version)
    elif args.command == "normalize":
        print(normalize_version(args.version))
    else:
        raise ValueError(f"Invalid command: {args.command}")

if __name__ == "__main__":
    main()
