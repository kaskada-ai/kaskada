import tomlkit
import argparse
from typing import List

def get_value_from_toml(file_path: str, toml_path: List[str]) -> str:
    """Retrieve a value from a TOML file at the given path."""
    with open(file_path, 'r') as f:
        data = tomlkit.parse(f.read())

    temp = data
    for key in toml_path:
        temp = temp[key]

    return str(temp)  # Convert value to string in case it's a number or boolean

def main():
    parser = argparse.ArgumentParser(description='Retrieve value from a TOML file.')
    parser.add_argument('file', type=str, help='Path to the TOML file.')
    parser.add_argument('path', type=str, help='Path within the TOML file (e.g., package.version)')

    args = parser.parse_args()

    toml_path = args.path.split('.')
    value = get_value_from_toml(args.file, toml_path)
    print(f"{value}")

if __name__ == "__main__":
    main()
