from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from quartodoc import blueprint, collect, layout
from quartodoc.validation import fmt


_log = logging.getLogger("quartodoc")


def load_layout(sections: dict, package: str, options=None):
    try:
        return layout.Layout(sections=sections, package=package, options=options)
    except ValidationError as e:
        msg = "Configuration error for YAML:\n - "
        errors = [fmt(err) for err in e.errors() if fmt(err)]
        first_error = errors[
            0
        ]  # we only want to show one error at a time b/c it is confusing otherwise
        msg += first_error
        raise ValueError(msg) from None


class Linter:
    """Base class for linting API docs.

    Parameters
    ----------
    package: str
        The name of the package.
    sections: ConfigSection
        A list of sections, with items to document.
    options:
        Default options to set for all pieces of content (e.g. include_attributes).
    source_dir:
        A directory where source files to be documented live. This is only necessary
        if you are not documenting a package, but collection of scripts. Use a "."
        to refer to the current directory.
    parser:
        Docstring parser to use. This correspond to different docstring styles,
        and can be one of "google", "sphinx", and "numpy". Defaults to "numpy".

    """

    package: str
    sections: list[Any]
    options: dict | None
    source_dir: str | None
    parser: str

    def __init__(
        self,
        package: str,
        sections: list[Any] = tuple(),
        options: dict | None = None,
        source_dir: str | None = None,
        parser="google",
    ):
        self.package = package
        self.sections = sections
        self.options = options
        self.parser = parser

        if source_dir:
            self.source_dir = str(Path(source_dir).absolute())
            sys.path.append(self.source_dir)

    def get_items(self, use_sections: bool):
        sections = self.sections if use_sections else []

        layout = load_layout(
            sections=sections, package=self.package, options=self.options
        )

        _, items = collect(blueprint(layout, parser=self.parser), base_dir="")

        return [item.name for item in items]

    def lint(self):
        """Lints the config and lets you know about any missing items"""

        ref_items = self.get_items(True)
        pkg_items = self.get_items(False)

        issue_count = 0
        for pkg_item in pkg_items:
            if pkg_item not in ref_items:
                _log.warning(f"Missing item: {pkg_item}")
                issue_count += 1

        if issue_count > 0:
            _log.error("Encountered un-documented items. Please fix.")
            sys.exit(1)

    @classmethod
    def from_quarto_config(cls, quarto_cfg: "str | dict"):
        """Construct a Builder from a configuration object (or yaml file)."""

        # TODO: validation / config model loading
        if isinstance(quarto_cfg, str):
            import yaml

            quarto_cfg = yaml.safe_load(open(quarto_cfg))

        cfg = quarto_cfg.get("quartodoc")
        if cfg is None:
            raise KeyError("No `quartodoc:` section found in your _quarto.yml.")

        return Linter(
            **{
                k: v
                for k, v in cfg.items()
                if k in ["package", "sections", "options", "parser"]
            },
        )
