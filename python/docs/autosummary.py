from __future__ import annotations

import logging
import yaml

from fnmatch import fnmatchcase
from pathlib import Path
from pydantic import ValidationError

from quartodoc.inventory import create_inventory, convert_inventory
from quartodoc import layout, preview
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

        _log.info("Writing pages")
        self.write_pages(blueprint)

        # _log.info("Writing docs pages")
        # self.write_doc_pages(pages, filter, hierarchy)

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

    def write_pages(self, blueprint: layout.Layout):
        root = layout.Section(
            title=self.title,
            desc="This is the API Reference",
        )

        root_text = self.renderer.render(root)
        root_path = Path(self.dir) / self.out_index
        self.write_page_if_not_exists(root_path, root_text)

        last_title = None
        order = 1

        for section in blueprint.sections:
            if section.title:
                last_title = section.title
                section_text = self.renderer.render(section, order=order)
                order += 1
                location = Path(self.dir) / section.title
            elif section.subtitle:
                section_text = self.renderer.render(section)
                location = Path(self.dir) / last_title / section.subtitle

            section_path = location / self.out_index
            self.write_page_if_not_exists(section_path, section_text)

            for item in section.contents:
                if isinstance(item, layout.Page):
                    _log.info(f"Rendering {item.path}")
                    preview(item, max_depth=4)
                    page_text = self.renderer.render(item)
                    page_path = location / (item.path + self.out_page_suffix)
                    self.write_page_if_not_exists(page_path, page_text)

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

    def write_page_if_not_exists(self, path: Path, content):
        if (
            self.rewrite_all_pages
            or (not path.exists())
            or (path.read_text() != content)
        ):
            _log.info(f"Writing: {path}")
            path.parent.mkdir(exist_ok=True, parents=True)
            path.write_text(content)
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
    style = "pkgdown"


if __name__ == "__main__":
    _enable_logs()
    b = BuilderPkgdown.from_quarto_config("_quartodoc.yml")
    b.build()
