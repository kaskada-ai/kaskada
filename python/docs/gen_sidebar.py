from __future__ import annotations

import logging
import yaml
import sys

from pathlib import Path
from pydantic import ValidationError

from quartodoc.validation import fmt
from quartodoc import blueprint, layout, preview

from typing import Any


def _enable_logs():
    import logging
    import sys

    root = logging.getLogger("quartodoc")
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)


class SideBar:
    """Class for building the SideBar.

    Parameters
    ----------
    package: str
        The name of the package.
    sections: ConfigSection
        A list of sections, with items to document.
    dir:
        Name of API directory.
    options:
        Default options to set for all pieces of content (e.g. include_attributes).
    sidebar:
        The output path for a sidebar yaml config (by default no config generated).
    source_dir:
        A directory where source files to be documented live. This is only necessary
        if you are not documenting a package, but collection of scripts. Use a "."
        to refer to the current directory.
    dynamic:
        Whether to dynamically load all python objects. By default, objects are
        loaded using static analysis.
    parser:
        Docstring parser to use. This correspond to different docstring styles,
        and can be one of "google", "sphinx", and "numpy". Defaults to "numpy".

    """

    # misc config
    out_page_suffix = ".qmd"
    package: str
    dir: str

    def __init__(
        self,
        package: str,
        sections: "list[Any]" = tuple(),
        options: "dict | None" = None,
        dir: str = "reference",
        sidebar: "str | None" = None,
        source_dir: "str | None" = None,
        dynamic: bool | None = None,
        parser="numpy",
    ):
        self.package = package
        self.dir = dir
        self.sidebar = sidebar
        self.parser = parser
        self.source_dir = str(Path(source_dir).absolute()) if source_dir else None
        self.dynamic = dynamic
        self.sections = sections
        self.options = options


    def build(self, filter: str = "*"):
        if self.source_dir:
            sys.path.append(self.source_dir)

        _log = logging.getLogger("quartodoc")

        _log.info("Generating layout.")
        layout = self.load_layout(sections=self.sections, package=self.package, options=self.options)

        _log.info("Generating blueprint.")
        bp = blueprint(layout, dynamic=self.dynamic, parser=self.parser)
        #preview(bp, max_depth=5)

        _log.info("Generating sidebar.")
        d_sidebar = self._generate_sidebar(bp)
        yaml.dump(d_sidebar, open(self.sidebar, "w"))


    def load_layout(self, sections: dict, package: str, options=None):
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

# maybe don't even need to do this.  maybe can do completely with render,
# if it puts things in folders based on my _quartodoc config layout

    def _generate_sidebar(self, blueprint: layout.Layout):
        contents = [f"{self.dir}/index{self.out_page_suffix}"]
        in_subsection = False
        crnt_entry = {}
        for section in blueprint.sections:
            if section.title:
                if crnt_entry:
                    contents.append(crnt_entry)

                in_subsection = False
                crnt_entry = {"section": section.title, "contents": []}
            elif section.subtitle:
                in_subsection = True

            links = []
            for entry in section.contents:
                links.extend(self._page_to_links(entry))

            if in_subsection:
                sub_entry = {"section": section.subtitle, "contents": links}
                crnt_entry["contents"].append(sub_entry)
            else:
                crnt_entry["contents"].extend(links)

        if crnt_entry:
            contents.append(crnt_entry)

        entries = [{"id": self.dir, "contents": contents}, {"id": "dummy-sidebar"}]
        return {"website": {"sidebar": entries}}

    def _page_to_links(self, el: layout.Page) -> list[str]:
        # if el.flatten:
        #     links = []
        #     for entry in el.contents:
        #         links.append(f"{self.dir}/{entry.path}{self.out_page_suffix}")
        #     return links

        return [f"{self.dir}/{el.path}{self.out_page_suffix}"]

def from_quarto_config(quarto_cfg: "str | dict"):
    """Construct a Builder from a configuration object (or yaml file)."""

    if isinstance(quarto_cfg, str):
        quarto_cfg = yaml.safe_load(open(quarto_cfg))

    cfg = quarto_cfg.get("quartodoc")
    if cfg is None:
        raise KeyError("No `quartodoc:` section found in your _quarto.yml.")

    return SideBar(
        **{k: v for k, v in cfg.items() if k in ["package", "sections", "dir", "options", "sidebar", "source_dir", "dynamic", "parser"]}
    )

if __name__ == "__main__":
    _enable_logs()
    from_quarto_config("_quartodoc.old.yml").build()
