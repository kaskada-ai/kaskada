import tomlkit
from tomlkit import dumps
import argparse
from collections import defaultdict
from typing import Dict, List

def update_version_in_data(data: Dict, version: str, toml_paths: List[str]) -> None:
    """Update the version number in a data dictionary (parsed TOML) at multiple paths."""
    for path in toml_paths:
        temp = data
        path = path.split('.')
        for key in path[:-1]:
            temp = temp[key]
        temp[path[-1]] = version

def main():
    parser = argparse.ArgumentParser(description='Update version in TOML files.')
    parser.add_argument('version', type=str, help='The version number to set.')
    parser.add_argument('entries', nargs='+', type=str,
                        help='TOML file and path, format: <file_path>:<toml_path> (e.g., config.toml:package.version)')

    args = parser.parse_args()

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

if __name__ == "__main__":
    main()
