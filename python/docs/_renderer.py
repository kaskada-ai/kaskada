from __future__ import annotations

import quartodoc.ast as qast

from griffe.docstrings import dataclasses as ds
from griffe import dataclasses as dc
from tabulate import tabulate
from plum import dispatch
from typing import Union, Optional
from quartodoc import layout

from _base import BaseRenderer


try:
    # Name and Expression were moved to expressions in v0.28
    from griffe import expressions as expr
except ImportError:
    from griffe import dataclasses as expr

skip_annotation_types = [
    "kaskada",
    "kaskada.destination",
    "kaskada.results",
    "kaskada.windows",
    "pyarrow",
]


def _has_attr_section(el: dc.Docstring | None):
    if el is None:
        return False

    return any([isinstance(x, ds.DocstringSectionAttributes) for x in el.parsed])


def escape(val: str):
    return f"`{val}`"


def sanitize(val: str, allow_markdown=False):
    # sanitize common tokens that break tables
    res = val.replace("\n", " ").replace("|", "\\|")

    # sanitize elements that can get interpreted as markdown links
    # or citations
    if not allow_markdown:
        return res.replace("[", "\\[").replace("]", "\\]")

    return res


class Renderer(BaseRenderer):
    """Render docstrings to markdown.

    Parameters
    ----------
    show_signature_annotations: bool
        Whether to show annotations in the function signature.
    """

    style = "markdown"

    def __init__(
        self,
        show_signature_annotations: bool = False,
        hook_pre=None,
    ):
        self.show_signature_annotations = show_signature_annotations
        self.hook_pre = hook_pre

    def _fetch_object_dispname(self, el: "dc.Alias | dc.Object"):
        parts = el.path.split(".")[1:]
        name = parts.pop()
        prefix = ".".join(parts)
        dispname = f"**{prefix}.**[**{name}**]{{.red}}"

        if isinstance(el, dc.Object):
            if 'staticmethod' in el.labels:
                dispname = "***static*** " + dispname

        text = [dispname]

        # if isinstance(el, dc.Object) and el.kind == dc.Kind.CLASS:
        #     text.append(f"Bases: []({el.parent.name})")

        return "\n\n".join(text)

    def _fetch_method_parameters(self, el: dc.Function):
        # adapted from mkdocstrings-python jinja tempalate
        if el.parent and el.parent.is_class and len(el.parameters) > 0:
            if el.parameters[0].name in {"self", "cls"}:
                return dc.Parameters(*list(el.parameters)[1:])

        return el.parameters

    def _render_definition_list(self, title: str, items: [str], title_class: Optional[str] = None) -> str:
       # title = f'{{.{title_class}}}{title_text}' if title_class else title_text
        rows = [title]
        for item in items:
            if len(rows) == 1:
                rows.append(f'~   {item}')
            else:
                rows.append(f'    {item}')
        if title_class:
            rows.insert(0, f':::{{.{title_class}}}')
            rows.append(':::')
        return "\n" + "\n".join(rows)

    def _render_header(self, title: str, order: Optional[int] = None) -> str:
        text = ["---"]
        text.append(f'title: {title}')
        if order:
            text.append(f'order: {order}')
        text.append("---")
        return "\n".join(text)

    def _render_table(self, rows, headers):
        table = tabulate(rows, headers=headers, tablefmt="github")

        return table

    # render_annotation method --------------------------------------------------------

    @dispatch
    def render_annotation(self, el: str) -> str:
        return sanitize(el)

    @dispatch
    def render_annotation(self, el: None) -> str:
        return ""

    @dispatch
    def render_annotation(self, el: expr.Name) -> str:
        if el.full not in skip_annotation_types:
            return f"[{sanitize(el.source)}](`{el.full}`)"
        return ""

    @dispatch
    def render_annotation(self, el: expr.Expression) -> str:
        text = "".join(map(self.render_annotation, el))
        return text.lstrip(".")

    # signature method --------------------------------------------------------

    @dispatch
    def signature(self, el: layout.Doc):
        return self.signature(el.obj)

    @dispatch
    def signature(self, el: dc.Alias, source: Optional[dc.Alias] = None):
        """Return a string representation of an object's signature."""
        return self.signature(el.target, el)

    @dispatch
    def signature(self, el: dc.Function, source: Optional[dc.Alias] = None) -> str:
        name = self._fetch_object_dispname(source or el)
        pars = self.render(self._fetch_method_parameters(el))
        return f"{name}([{pars}]{{.bold-italic}})"

    @dispatch
    def signature(self, el: dc.Class, source: Optional[dc.Alias] = None) -> str:
        name = self._fetch_object_dispname(source or el)
        return f"***class*** {name}"

    @dispatch
    def signature(
        self, el: Union[dc.Module, dc.Attribute], source: Optional[dc.Alias] = None
    ):
        name = self._fetch_object_dispname(source or el)
        return f"`{name}`"

    # render method -----------------------------------------------------------

    @dispatch
    def render(self, el):
        """Return a string representation of an object, or layout element."""

        raise NotImplementedError(f"Unsupported type: {type(el)}")

    @dispatch
    def render(self, el: str):
        return el

    # render layouts ==========================================================

    @dispatch
    def render(self, el: layout.Section, order: Optional[int] = None) -> str:
        rows = [self._render_header(el.title or el.subtitle, order=order)]

        if el.desc:
            rows.append(el.desc)

        text = "\n\n".join(rows)

        if len(el.contents) > 0:
            text += "\n\n" + self.summarize(el.contents)

        return text

    @dispatch
    def render(self, el: layout.Page):
        rows = []
        if el.summary:
            if el.summary.name:
                rows.append(self._render_header(el.summary.name))
            if el.summary.desc:
                rows.append(sanitize(el.summary.desc, allow_markdown=True))

        for item in el.contents:
            rows.append(self.render(item))

        return "\n\n".join(rows)

    @dispatch
    def render(self, el: layout.Doc):
        raise NotImplementedError(f"Unsupported Doc type: {type(el)}")

    @dispatch
    def render(self, el: Union[layout.DocClass, layout.DocModule], single_page: bool = False) -> str:
        title = self._render_header(el.name)

        sig = self.signature(el)
        body_rows = self.render(el.obj).split("\n")

        if el.members:
            # add attributes
            # skip if docstring has an attributes section
            raw_attrs = [x for x in el.members if x.obj.is_attribute]
            if raw_attrs and not _has_attr_section(el.obj.docstring):
                attr_rows = map(self.render, raw_attrs)
                attr_text = self._render_definition_list(
                    "Attributes:", attr_rows, title_class="highlight")
                body_rows.extend(attr_text.split("\n"))

            # add classes
            for raw_class in el.members:
                if raw_class.obj.is_class and isinstance(raw_class, layout.Doc):
                    body_rows.extend(self.render(raw_class, single_page=True).split("\n"))

            # add methods
            for raw_method in el.members:
                if raw_method.obj.is_function and isinstance(raw_method, layout.Doc):
                    body_rows.extend(self.render(raw_method, single_page=True).split("\n"))

        text = self._render_definition_list(sig, body_rows)

        return "\n\n".join([title, text])

    @dispatch
    def render(self, el: layout.DocFunction, single_page: bool = False):
        title = "" if single_page else self._render_header(el.name)

        sig = self.signature(el)
        body_rows = self.render(el.obj).split("\n")
        text = self._render_definition_list(sig, body_rows)

        return "\n\n".join([title, text])

    @dispatch
    def render(self, el: layout.DocAttribute, single_page: bool = False):
        link = f"[{el.name}](#{el.anchor})"
        description = self.summarize(el.obj)

        return " -- ".join([link, description])

    # render griffe objects ===================================================

    @dispatch
    def render(self, el: Union[dc.Object, dc.Alias]):
        """Render high level objects representing functions, classes, etc.."""

        str_body = []
        if el.docstring is None:
            pass
        else:
            patched_sections = qast.transform(el.docstring.parsed)
            for section in patched_sections:
                str_body.append(self.render(section))

        parts = [*str_body]

        return "\n\n".join(parts)

    # signature parts -------------------------------------------------------------

    @dispatch
    def render(self, el: dc.Parameters):
        # index for switch from positional to kw args (via an unnamed *)
        try:
            kw_only = [par.kind for par in el].index(dc.ParameterKind.keyword_only)
        except ValueError:
            kw_only = None

        # index for final positionly only args (via /)
        try:
            pos_only = max(
                [
                    ii
                    for ii, el in enumerate(el)
                    if el.kind == dc.ParameterKind.positional_only
                ]
            )
        except ValueError:
            pos_only = None

        pars = list(map(self.render, el))

        # insert a single `*,` argument to represent the shift to kw only arguments,
        # only if the shift to kw_only was not triggered by *args (var_positional)
        if (
            kw_only is not None
            and kw_only > 0
            and el[kw_only - 1].kind != dc.ParameterKind.var_positional
        ):
            pars.insert(kw_only, sanitize("*"))

        # insert a single `/, ` argument to represent shift from positional only arguments
        # note that this must come before a single *, so it's okay that both this
        # and block above insert into pars
        if pos_only is not None:
            pars.insert(pos_only + 1, sanitize("/"))

        return ", ".join(pars)

    @dispatch
    def render(self, el: dc.Parameter):
        splats = {dc.ParameterKind.var_keyword, dc.ParameterKind.var_positional}
        has_default = el.default and el.kind not in splats

        if el.kind == dc.ParameterKind.var_keyword:
            glob = "**"
        elif el.kind == dc.ParameterKind.var_positional:
            glob = "*"
        else:
            glob = ""

        annotation = self.render_annotation(el.annotation)
        name = sanitize(el.name)

        if self.show_signature_annotations:
            if annotation and has_default:
                res = f"{glob}{name}: {annotation} = {el.default}"
            elif annotation:
                res = f"{glob}{name}: {annotation}"
        elif has_default:
            res = f"{glob}{name}={el.default}"
        else:
            res = f"{glob}{name}"
        return res

    # docstring parts -------------------------------------------------------------

    # text ----
    # note this can be a number of things. for example, opening docstring text,
    # or a section with a header not included in the numpydoc standard
    @dispatch
    def render(self, el: ds.DocstringSectionText):
        new_el = qast.transform(el)
        if isinstance(new_el, ds.DocstringSectionText):
            # ensures we don't recurse forever
            return el.value

        return self.render(new_el)

    # parameters ----

    @dispatch
    def render(self, el: ds.DocstringSectionParameters):
        # if more than one param, render as un-ordered list
        prefix = "* " if len(el.value) > 1 else ""
        follow = "  " if len(el.value) > 1 else ""

        rows = []
        for param in el.value:
            name = sanitize(param.name)
            anno = self.render_annotation(param.annotation)
            default = f', default: {escape(param.default)}' if param.default else ""

            rows.append(f'{prefix}**{name}** ({anno}{default})')
            rows.append("")
            for row in param.description.split("\n"):
                rows.append(f'{follow}{row}')
            rows.append("")

        return self._render_definition_list("Parameters:", rows, title_class="highlight")

    # attributes ----

    @dispatch
    def render(self, el: ds.DocstringSectionAttributes):
        # if more than one param, render as un-ordered list
        prefix = "* " if len(el.value) > 1 else ""
        follow = "  " if len(el.value) > 1 else ""

        rows = []
        for attr in el.value:
            name = sanitize(attr.name)
            anno = self.render_annotation(attr.annotation)
            rows.append(f'{prefix}**{name}** ({anno})')
            rows.append("")
            for row in attr.description.split("\n"):
                rows.append(f'{follow}{row}')
            rows.append("")

        return self._render_definition_list("Attributes:", rows, title_class="highlight")

    # examples ----

    @dispatch
    def render(self, el: ds.DocstringSectionExamples):
        # its value is a tuple: DocstringSectionKind["text" | "examples"], str
        data = map(qast.transform, el.value)
        return "\n\n".join(list(map(self.render, data)))

    @dispatch
    def render(self, el: qast.ExampleCode):
        return f"""```python
{el.value}
```"""

    @dispatch
    def render(self, el: qast.ExampleText):
        return el.value

    # returns ----

    @dispatch
    def render(self, el: ds.DocstringSectionReturns):
        # if more than one param, render as un-ordered list
        prefix = "* " if len(el.value) > 1 else ""
        follow = "  " if len(el.value) > 1 else ""

        rows = []
        for item in el.value:
            title = prefix
            name = sanitize(item.name)
            if name:
                title += f'**{name}**'

            return_type = self.render_annotation(item.annotation)
            if return_type:
                title += return_type

            if title != prefix:
                rows.append(title)

            if item.description:
                rows.append("")
                for row in item.description.split("\n"):
                    rows.append(f'{follow}{row}')
                rows.append("")

        return self._render_definition_list("Returns:", rows, title_class="highlight")

    @dispatch
    def render(self, el: ds.DocstringSectionRaises):
        # if more than one param, render as un-ordered list
        prefix = "* " if len(el.value) > 1 else ""
        follow = "  " if len(el.value) > 1 else ""

        rows = []
        for item in el.value:
            # name = sanitize(item.name)
            anno = self.render_annotation(item.annotation)
            rows.append(f'{prefix}{anno}')
            rows.append("")
            for row in item.description.split("\n"):
                rows.append(f'{follow}{row}')
            rows.append("")

        return self._render_definition_list("Raises:", rows, title_class="highlight")

    @dispatch
    def render(self, el: ds.DocstringSectionAdmonition) -> str:
        rows = []
        if el.title.lower().startswith("note"):
            rows.append(f'::: {{.callout-note title="{el.title}"}}')
        elif el.title.lower().startswith("warn"):
            rows.append(f'::: {{.callout-warning title="{el.title}"}}')
        else:
            rows.append(f'::: {{.callout-tip title="{el.title}"}}')

        rows.append(el.value.description)
        rows.append(':::')

        return "\n".join(rows)

    # unsupported parts ----

    @dispatch.multi(
        (ds.DocstringAdmonition,),
        (ds.DocstringDeprecated,),
        (ds.DocstringWarn,),
        (ds.DocstringYield,),
        (ds.DocstringReceive,),
        (ds.DocstringAttribute,),
    )
    def render(self, el):
        raise NotImplementedError(f"{type(el)}")

    # Summarize ===============================================================
    # this method returns a summary description, such as a table

    @staticmethod
    def _summary_row(link, description):
        return f"| {link} | {sanitize(description, allow_markdown=True)} |"

    @dispatch
    def summarize(self, el):
        """Produce a summary table."""

        raise NotImplementedError("Unsupported type: {type(el)}")

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
