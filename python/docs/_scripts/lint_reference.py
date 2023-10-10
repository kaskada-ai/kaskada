import logging
import sys

from linter import Linter


if __name__ == "__main__":
    root = logging.getLogger("quartodoc")
    root.setLevel(logging.WARNING)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    Linter.from_quarto_config("_reference.yml").lint()
