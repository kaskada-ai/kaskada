from __future__ import annotations

import inspect
import logging
import warnings
import yaml

from fnmatch import fnmatchcase
from griffe.loader import GriffeLoader
from griffe.collections import ModulesCollection, LinesCollection
from griffe.dataclasses import Alias
from griffe.docstrings.parsers import Parser, parse
from griffe.docstrings import dataclasses as ds  # noqa
from griffe import dataclasses as dc
from plum import dispatch  # noqa
from pathlib import Path
from types import ModuleType
from pydantic import ValidationError

from quartodoc.inventory import create_inventory, convert_inventory
from quartodoc import layout, preview
from quartodoc.parsers import get_parser_defaults
from quartodoc.renderers import Renderer
from quartodoc.validation import fmt

from typing import Any, Union, Optional


def _enable_logs():
    import logging
    import sys

    root = logging.getLogger("quartodoc")
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)


_log = logging.getLogger("quartodoc")


# pkgdown =====================================================================


# TODO: styles -- pkgdown, single-page, many-pages
class Builder:
    """Base class for building API docs.

    Parameters
    ----------
    package: str
        The name of the package.
    sections: ConfigSection
        A list of sections, with items to document.
    version:
        The package version. By default this attempts to look up the current package
        version (TODO).
    dir:
        Name of API directory.
    title:
        Title of the API index page.
    renderer: Renderer
        The renderer used to convert docstrings (e.g. to markdown).
    options:
        Default options to set for all pieces of content (e.g. include_attributes).
    out_index:
        The output path of the index file, used to list all API functions.
    sidebar:
        The output path for a sidebar yaml config (by default no config generated).
    rewrite_all_pages:
        Whether to rewrite all rendered doc pages, or only those with changes.
    source_dir:
        A directory where source files to be documented live. This is only necessary
        if you are not documenting a package, but collection of scripts. Use a "."
        to refer to the current directory.
    dynamic:
        Whether to dynamically load all python objects. By default, objects are
        loaded using static analysis.
    render_interlinks:
        Whether to render interlinks syntax inside documented objects. Note that the
        interlinks filter is required to generate the links in quarto.
    parser:
        Docstring parser to use. This correspond to different docstring styles,
        and can be one of "google", "sphinx", and "numpy". Defaults to "numpy".

    """

    # builder dispatching ----
    style: str
    _registry: "dict[str, Builder]" = {}

    # misc config
    out_inventory: str = "objects.json"
    out_index: str = "index.qmd"
    out_page_suffix = ".qmd"

    # quarto yaml config -----
    # TODO: add model for section with the fields:
    # title, desc, contents: list[str]
    package: str
    version: "str | None"
    dir: str
    title: str

    renderer: Renderer

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if cls.style in cls._registry:
            raise KeyError(f"A builder for style {cls.style} already exists")

        cls._registry[cls.style] = cls

    def __init__(
        self,
        package: str,
        # TODO: correct typing
        sections: "list[Any]" = tuple(),
        options: "dict | None" = None,
        version: "str | None" = None,
        dir: str = "reference",
        title: str = "Function reference",
        renderer: "dict | Renderer | str" = "markdown",
        out_index: str = None,
        sidebar: "str | None" = None,
        rewrite_all_pages=False,
        source_dir: "str | None" = None,
        dynamic: bool | None = None,
        parser="numpy",
        render_interlinks: bool = False,
        _fast_inventory=False,
    ):
        self.layout = self.load_layout(
            sections=sections, package=package, options=options
        )

        self.package = package
        self.version = None
        self.dir = dir
        self.title = title
        self.sidebar = sidebar
        self.parser = parser

        self.renderer = Renderer.from_config(renderer)
        if render_interlinks:
            # this is a top-level option, but lives on the renderer
            # so we just manually set it there for now.
            self.renderer.render_interlinks = render_interlinks

        if out_index is not None:
            self.out_index = out_index

        self.rewrite_all_pages = rewrite_all_pages
        self.source_dir = str(Path(source_dir).absolute()) if source_dir else None
        self.dynamic = dynamic

        self._fast_inventory = _fast_inventory

    def load_layout(self, sections: dict, package: str, options=None):
        # TODO: currently returning the list of sections, to make work with
        # previous code. We should make Layout a first-class citizen of the
        # process.
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

    # building ----------------------------------------------------------------

    def build(self, filter: str = "*"):
        """Build index page, sphinx inventory, and individual doc pages.

        Parameters
        ----------
        filter:
            A simple pattern, that may include * as a wildcard. If specified,
            only doc paths for objects with matching names will be written.
            Path is the file's base name in the API dir (e.g. MdRenderer.render)
        """

        from quartodoc import blueprint, collect

        if self.source_dir:
            import sys

            sys.path.append(self.source_dir)

        # shaping and collection ----

        _log.info("Generating blueprint.")
        blueprint = blueprint(self.layout, dynamic=self.dynamic, parser=self.parser)

        hierarchy = self.gen_hierarchy(blueprint)

        _log.info("Collecting pages and inventory items.")
        pages, items = collect(blueprint, base_dir=self.dir)

        # writing pages ----

        _log.info("Writing indexes")
        self.write_indexes(blueprint)

        _log.info("Writing docs pages")
        self.write_doc_pages(pages, filter, hierarchy)

        # inventory ----

        # update paths in items
        for item in items:
            uri = item.uri
            uri = uri.removeprefix(f'{self.dir}/')

            path = uri.split('#')[0] if "#" in uri else uri
            fragment = uri.split('#')[1] if "#" in uri else ""

            location = None
            uri_lookup = f'kaskada.{path.removesuffix(".html")}'
            prefix_lookup = f".".join(item.name.split(".")[:-1])
            if uri_lookup in hierarchy:
                location = hierarchy[uri_lookup]
            elif prefix_lookup in hierarchy:
                location = hierarchy[prefix_lookup]
            elif item.name in hierarchy:
                location = hierarchy[item.name]
            else:
                print(
                    f'Lost Item: {item}, uri_lookup: {uri_lookup}, prefix_lookup: {prefix_lookup}')

            if location:
                item.uri = f'{self.dir}/{location}/{path}#{fragment}'

        _log.info("Creating inventory file")
        inv = self.create_inventory(items)
        if self._fast_inventory:
            # dump the inventory file directly as text
            # TODO: copied from __main__.py, should add to inventory.py
            import sphobjinv as soi

            df = inv.data_file()
            soi.writebytes(Path(self.out_inventory).with_suffix(".txt"), df)

        else:
            convert_inventory(inv, self.out_inventory)

        # sidebar ----

        if self.sidebar:
            _log.info(f"Writing sidebar yaml to {self.sidebar}")
            self.write_sidebar(blueprint)

    def get_package(self, item: Union[layout.Section, layout.Page]) -> str:
        if item.package and f'{item.package}' != "":
            return item.package
        else:
            return self.package

    def gen_hierarchy(self, blueprint: layout.Layout) -> {str: str}:
        last_title = None
        hierarchy = {}

        for section in blueprint.sections:
            # preview(section, max_depth=4)
            # print()

            if section.title:
                last_title = section.title
                location = section.title
            elif section.subtitle:
                location = f'{last_title}/{section.subtitle}'

            for item in section.contents:
                hierarchy[f'{self.get_package(section)}.{item.path}'] = location
        # print(hierarchy)
        return hierarchy

    def header(self, title: str, order: Optional[str] = None) -> str:
        text = ["---"]
        text.append(f'title: {title}')
        if order:
            text.append(f'order: {order}')
        text.append("---")
        return "\n".join(text) + "\n\n"

    def write_indexes(self, blueprint: layout.Layout):
        """Write index pages for all sections"""

        # --- root index
        text = self.header(self.title)
        text += "This is the API Reference"

        p_index = Path(self.dir) / self.out_index
        p_index.parent.mkdir(exist_ok=True, parents=True)
        p_index.write_text(text)

        # --- section indexes
        last_title = None
        order = 1

        for section in blueprint.sections:
            if section.title:
                last_title = section.title
                text = self.header(section.title, order=order)
                order += 1
                p_index = Path(self.dir) / section.title / self.out_index
            elif section.subtitle:
                text = self.header(section.subtitle)
                p_index = Path(self.dir) / last_title / section.subtitle / self.out_index

            if section.desc:
                text += section.desc + "\n\n"

            if section.contents:
                text += self.renderer.summarize(section.contents)

            p_index.parent.mkdir(exist_ok=True, parents=True)
            p_index.write_text(text)

        return

    def write_index_old(self, blueprint: layout.Layout):
        """Write API index page."""

        _log.info("Summarizing docs for index page.")
        content = self.renderer.summarize(blueprint)
        _log.info(f"Writing index to directory: {self.dir}")

        final = f"# {self.title}\n\n{content}"

        p_index = Path(self.dir) / self.out_index
        p_index.parent.mkdir(exist_ok=True, parents=True)
        p_index.write_text(final)

        return str(p_index)

    def write_doc_pages(self, pages: [layout.Page], filter: str, hierarchy: {}):
        """Write individual function documentation pages."""

        for page in pages:
            _log.info(f"Rendering {page.path}")
            # preview(page)
            rendered = self.renderer.render(page)

            try:
                location = hierarchy[f'{self.get_package(page)}.{page.path}']
            except:
                location = hierarchy[page.contents[0].anchor]

            html_path = Path(self.dir) / location / (page.path + self.out_page_suffix)
            html_path.parent.mkdir(exist_ok=True, parents=True)

            # Only write out page if it has changed, or we've set the
            # rewrite_all_pages option. This ensures that quarto won't have
            # to re-render every page of the API all the time.
            if filter != "*":
                is_match = fnmatchcase(page.path, filter)

                if is_match:
                    _log.info("Matched filter")
                else:
                    _log.info("Skipping write (no filter match)")
                    continue

            if (
                self.rewrite_all_pages
                or (not html_path.exists())
                or (html_path.read_text() != rendered)
            ):
                _log.info(f"Writing: {page.path}")
                html_path.write_text(rendered)
            else:
                _log.info("Skipping write (content unchanged)")

    # inventory ----

    def create_inventory(self, items):
        """Generate sphinx inventory object."""

        # TODO: get package version
        _log.info("Creating inventory")
        version = "0.0.9999" if self.version is None else self.version
        inventory = create_inventory(self.package, version, items)

        return inventory

    # sidebar ----

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

    def write_sidebar(self, blueprint: layout.Layout):
        """Write a yaml config file for API sidebar."""

        d_sidebar = self._generate_sidebar(blueprint)
        yaml.dump(d_sidebar, open(self.sidebar, "w"))

    def _page_to_links(self, el: layout.Page) -> list[str]:
        # if el.flatten:
        #     links = []
        #     for entry in el.contents:
        #         links.append(f"{self.dir}/{entry.path}{self.out_page_suffix}")
        #     return links

        return [f"{self.dir}/{el.path}{self.out_page_suffix}"]

    # constructors ----

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
        style = cfg.get("style", "pkgdown")
        cls_builder = cls._registry[style]

        _fast_inventory = quarto_cfg.get("interlinks", {}).get("fast", False)

        return cls_builder(
            **{k: v for k, v in cfg.items() if k != "style"},
            _fast_inventory=_fast_inventory,
        )


class BuilderPkgdown(Builder):
    """Build an API in R pkgdown style."""

    style = "pkgdown"


if __name__ == "__main__":
    _enable_logs()
    b = BuilderPkgdown.from_quarto_config("_quartodoc.old.yml")
    b.build()
