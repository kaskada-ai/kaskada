from __future__ import annotations

from griffe.docstrings import dataclasses as ds
from griffe import dataclasses as dc
from plum import dispatch
from typing import Union, Optional
from quartodoc import layout


class Summarizer():
    """Summarize docstrings to markdown."""

    @staticmethod
    def _summary_row(link, description):
        return f"| {link} | {description} |"

    @dispatch
    def summarize(self, el):
        raise NotImplementedError(f"Unsupported type: {type(el)}")

    @dispatch
    def summarize(self, el: layout.Layout):
        rendered_sections = list(map(self.summarize, el.sections))
        return "\n\n".join(rendered_sections)

    @dispatch
    def summarize(self, el: layout.Section):
        desc = f"\n\n{el.desc}" if el.desc is not None else ""
        if el.title is not None:
            header = f"## {el.title}{desc}"
        elif el.subtitle is not None:
            header = f"### {el.subtitle}{desc}"
        else:
            header = ""

        if el.contents:
            return f"{header}\n\n{self.summarize(el.contents)}"

        return header

    @dispatch
    def summarize(self, contents: layout.ContentList):
        thead = "| | |\n| --- | --- |"

        rendered = []
        for child in contents:
            rendered.append(self.summarize(child))

        return "\n".join([thead, *rendered])

    @dispatch
    def summarize(self, el: layout.Page):
        if el.summary is not None:
            # TODO: assumes that files end with .qmd
            return self._summary_row(
                f"[{el.summary.name}]({el.path}.qmd)", el.summary.desc
            )

        if len(el.contents) > 1 and not el.flatten:
            raise ValueError(
                "Cannot summarize Page. Either set its `summary` attribute with name "
                "and description details, or set `flatten` to True."
            )

        else:
            rows = [self.summarize(entry, el.path) for entry in el.contents]
            return "\n".join(rows)

    @dispatch
    def summarize(self, el: layout.MemberPage):
        # TODO: model should validate these only have a single entry
        return self.summarize(el.contents[0], el.path, shorten=True)

    @dispatch
    def summarize(self, el: layout.Interlaced, *args, **kwargs):
        rows = [self.summarize(doc, *args, **kwargs) for doc in el.contents]

        return "\n".join(rows)

    @dispatch
    def summarize(
        self, el: layout.Doc, path: Optional[str] = None, shorten: bool = False
    ):
        # this is where summary page method links are created
        if path is None:
            link = f"[{el.name}](#{el.anchor})"
        else:
            # TODO: assumes that files end with .qmd
            link = f"[{el.name}]({path}.qmd#{el.anchor})"

        description = self.summarize(el.obj)
        return self._summary_row(link, description)

    @dispatch
    def summarize(self, el: layout.Link):
        description = self.summarize(el.obj)
        return self._summary_row(f"[](`{el.name}`)", description)

    @dispatch
    def summarize(self, obj: Union[dc.Object, dc.Alias]) -> str:
        """Test"""
        # get high-level description
        doc = obj.docstring
        if doc is None:
            docstring_parts = []
        else:
            docstring_parts = doc.parsed

        if len(docstring_parts) and isinstance(
            docstring_parts[0], ds.DocstringSectionText
        ):
            description = docstring_parts[0].value
            short = description.split("\n")[0]

            return short

        return ""
