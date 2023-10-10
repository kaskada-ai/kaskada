from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from quartodoc import blueprint, collect, layout
from quartodoc.inventory import convert_inventory, create_inventory
from quartodoc.validation import fmt
from renderer import Renderer
from summarizer import Summarizer


# `preview()` can be used to help debug doc generation.
# use it on a `section` or `page` element to see a visual
# representation of the element contents. Use the `max_depth`
# named param to limit how much is returned to stdout.
# from quartodoc import preview

_log = logging.getLogger("quartodoc")


class Builder:
    """Base class for building API docs.

    Parameters
    ----------
    package: str
        The name of the package.
    sections: ConfigSection
        A list of sections, with items to document.
    dir:
        Name of API directory.
    title:
        Title of the API index page.
    options:
        Default options to set for all pieces of content (e.g. include_attributes).
    rewrite_all_pages:
        Whether to rewrite all rendered doc pages, or only those with changes.
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
    out_inventory: str = "objects.json"
    out_index: str = "index.qmd"
    out_page_suffix = ".qmd"

    package: str
    dir: str
    title: str

    page_map: {str: layout.Page}
    item_map: {str: layout.Item}
    items: [layout.Item]

    blueprint: layout.Layout

    def __init__(
        self,
        package: str,
        sections: "list[Any]" = tuple(),
        dir: str = "reference",
        title: str = "Function reference",
        options: "dict | None" = None,
        rewrite_all_pages=False,
        source_dir: "str | None" = None,
        dynamic: bool | None = None,
        parser="google",
    ):
        self.layout = self.load_layout(
            sections=sections, package=package, options=options
        )

        self.package = package
        self.dir = dir
        self.title = title
        self.rewrite_all_pages = rewrite_all_pages
        self.renderer = Renderer()
        self.summarizer = Summarizer()

        if source_dir:
            self.source_dir = str(Path(source_dir).absolute())
            sys.path.append(self.source_dir)

        self.blueprint = blueprint(self.layout, dynamic=dynamic, parser=parser)

        pages, items = collect(self.blueprint, base_dir=self.dir)

        self.page_map = {}
        for page in pages:
            self.page_map[page.path] = page

        self.items = []
        self.item_map = {}
        for item in items:
            self.item_map[item.name] = item

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

    def build(self):
        """Build index page, sphinx inventory, and individual doc pages."""

        # writing pages ----

        _log.info("Writing pages")
        self.write_pages()

        # inventory ----

        _log.info("Creating inventory file")
        inv = create_inventory(self.package, "0.0.9999", self.items)
        convert_inventory(inv, self.out_inventory)

    def write_pages(self):
        root = layout.Section(
            title=self.title,
            desc="This is the API Reference",
        )

        root_text = self.renderer.render(root)
        root_path = Path(self.dir) / self.out_index
        self.write_page_if_not_exists(root_path, root_text)

        last_title = None
        order = 1

        for section in self.blueprint.sections:
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

            is_flat = (
                section.options
                and section.options.children == layout.ChoicesChildren.flat
            )

            for page in section.contents:
                if isinstance(page, layout.Page):
                    # don't make separate pages for flat sections
                    if not is_flat:
                        _log.info(f"Rendering {page.path}")
                        # preview(page, max_depth=4)
                        page_text = self.renderer.render(page)
                        page_path = location / (page.path + self.out_page_suffix)
                        self.write_page_if_not_exists(page_path, page_text)
                    if page.path in self.page_map:
                        del self.page_map[page.path]

                    self.update_page_items(page, location, is_flat)
                else:
                    raise NotImplementedError(f"Unsupported section item: {type(page)}")

        if len(self.page_map.keys()) > 0:
            _log.warning(f"Extra pages: {self.page_map.keys()}")
            _log.error(
                "Linking between pages may not work properly. Fix the issue and try again"
            )

        if len(self.item_map.keys()) > 0:
            _log.warning(f"Extra items: {self.item_map.keys()}")
            _log.error(
                "Linking between pages may not work properly. Fix the issue and try again"
            )

    def update_page_items(self, page: layout.Page, location: Path, is_flat: bool):
        for doc in page.contents:
            if isinstance(doc, layout.Doc):
                page_path = (
                    f"{location}/index.html"
                    if is_flat
                    else f"{location}/{page.path}.html"
                )
                self.update_items(doc, page_path)
            else:
                raise NotImplementedError(f"Unsupported page item: {type(doc)}")

    def update_items(self, doc: layout.Doc, page_path: str):
        name = doc.obj.path
        uri = f"{page_path}#{doc.anchor}"

        # item corresponding to the specified path ----
        # e.g. this might be a top-level import
        if name in self.item_map:
            item = self.item_map[name]
            item.uri = uri
            del self.item_map[name]
        else:
            item = layout.Item(uri=uri, name=name, obj=doc.obj, dispname=None)
            _log.warning(f"Missing item, adding it: {item}")
        self.items.append(item)

        canonical_path = doc.obj.canonical_path
        if name != canonical_path:
            # item corresponding to the canonical path ----
            # this is where the object is defined (which may be deep in a submodule)
            if canonical_path in self.item_map:
                item = self.item_map[canonical_path]
                item.uri = uri
                del self.item_map[canonical_path]
            else:
                item = layout.Item(
                    uri=uri, name=canonical_path, obj=doc.obj, dispname=name
                )
                _log.warning(f"Missing item, adding it: {item}")
            self.items.append(item)

        # recurse in 😊
        if isinstance(doc, layout.DocClass):
            for member in doc.members:
                self.update_items(member, page_path)

    def write_index_old(self, bp: layout.Layout):
        """Write API index page."""

        _log.info("Summarizing docs for index page.")
        content = self.summarizer.summarize(bp)
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

        return Builder(
            **{k: v for k, v in cfg.items()},
        )
