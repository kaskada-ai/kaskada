from __future__ import annotations

from typing import Optional, Union

import quartodoc.ast as qast
from griffe import dataclasses as dc
from griffe.docstrings import dataclasses as ds
from plum import dispatch
from quartodoc import layout
from summarizer import Summarizer
from tabulate import tabulate


try:
    # Name and Expression were moved to expressions in v0.28
    from griffe import expressions as expr
except ImportError:
    from griffe import dataclasses as expr

skip_annotation_types = [
    "kaskada",
    "kaskada.destinations",
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


class Renderer:
    """Render docstrings to markdown."""

    summarizer = Summarizer()

    def _get_display_name(self, el: "dc.Alias | dc.Object") -> str:
        parts = el.path.split(".")[1:]
        name = parts.pop()
        prefix = ".".join(parts) if len(parts) > 0 else "kaskada"
        display_name = f"**{prefix}.**[**{name}**]{{.red}}"

        if isinstance(el, dc.Object):
            if "staticmethod" in el.labels:
                display_name = "***static*** " + display_name

        text = [display_name]

        # if isinstance(el, dc.Object) and el.kind == dc.Kind.CLASS:
        #     text.append(f"Bases: []({el.parent.name})")

        return "\n\n".join(text)

    def _fetch_method_parameters(self, el: dc.Function):
        if el.parent and el.parent.is_class and len(el.parameters) > 0:
            if el.parameters[0].name in {"self", "cls"}:
                return dc.Parameters(*list(el.parameters)[1:])

        return el.parameters

    def _render_definition_list(
        self, title: str, items: [str], title_class: Optional[str] = None
    ) -> str:
        rows = [title]
        for item in items:
            if len(rows) == 1:
                rows.append(f"~   {item}")
            else:
                rows.append(f"    {item}")
        if title_class:
            rows.insert(0, f":::{{.{title_class}}}")
            rows.append(":::")
        text = "\n\n".join(rows)
        # fix extra indenting for nested definition lists
        return text.replace("\n\n    \n\n", "\n\n")

    def _render_header(self, title: str, order: Optional[int] = None) -> str:
        text = ["---"]
        text.append(f"title: {title}")
        if order:
            text.append(f"order: {order}")
        text.append("---")
        return "\n".join(text)

    def _render_table(self, rows, headers) -> str:
        table = tabulate(rows, headers=headers, tablefmt="github")

        return table

    # render_annotation method --------------------------------------------------------

    @dispatch
    def render_annotation(self, el: str) -> str:  # noqa: F811
        # hack to get Timestream in the correct format for the kaskada.Arg
        # alias docs
        if el == "'Timestream'":
            return "[Timestream](`kaskada.Timestream`)"
        return sanitize(el)

    @dispatch
    def render_annotation(self, el: None) -> str:  # noqa: F811
        return ""

    @dispatch
    def render_annotation(self, el: expr.Name) -> str:  # noqa: F811
        if el.full not in skip_annotation_types:
            return f"[{sanitize(el.source)}](`{el.full}`)"
        return ""

    @dispatch
    def render_annotation(self, el: expr.Expression) -> str:  # noqa: F811
        text = "".join(map(self.render_annotation, el))
        return text.lstrip(".")

    @dispatch
    def render_annotation(self, el: dc.Attribute) -> str:  # noqa: F811
        text = "".join(map(self.render_annotation, el.value))
        return text.lstrip(".")

    # signature method --------------------------------------------------------

    @dispatch
    def signature(self, el: layout.Doc) -> str:  # noqa: F811
        return self.signature(el.obj)

    @dispatch
    def signature(  # noqa: F811
        self, el: dc.Alias, source: Optional[dc.Alias] = None
    ) -> str:
        """Return a string representation of an object's signature."""
        return self.signature(el.target, el)

    @dispatch
    def signature(  # noqa: F811
        self, el: dc.Function, source: Optional[dc.Alias] = None
    ) -> str:
        name = self._get_display_name(source or el)
        pars = self.render(self._fetch_method_parameters(el))
        return f"{name}([{pars}]{{.bold-italic}})"

    @dispatch
    def signature(  # noqa: F811
        self, el: dc.Class, source: Optional[dc.Alias] = None
    ) -> str:
        name = self._get_display_name(source or el)
        return f"***class*** {name}"

    @dispatch
    def signature(  # noqa: F811
        self, el: Union[dc.Module, dc.Attribute], source: Optional[dc.Alias] = None
    ) -> str:
        name = self._get_display_name(source or el)
        return f"`{name}`"

    # render method -----------------------------------------------------------

    @dispatch
    def render(self, el) -> str:  # noqa: F811
        """Return a string representation of an object, or layout element."""

        raise NotImplementedError(f"Unsupported type: {type(el)}")

    @dispatch
    def render(self, el: str) -> str:  # noqa: F811
        return el

    # render layouts ==========================================================

    @dispatch
    def render(  # noqa: F811
        self, el: layout.Section, order: Optional[int] = None
    ) -> str:
        rows = [self._render_header(el.title or el.subtitle, order=order)]

        if el.desc:
            rows.append(el.desc)

        if el.options and el.options.children == layout.ChoicesChildren.flat:
            for page in el.contents:
                rows.append(self.render(page, is_flat=True))
            text = "\n\n".join(rows)
        else:
            text = "\n\n".join(rows)
            text += "\n\n" + self.summarizer.summarize(el.contents)

        return text

    @dispatch
    def render(self, el: layout.Page, is_flat: bool = False) -> str:  # noqa: F811
        rows = []
        if el.summary:
            if el.summary.name:
                if is_flat:
                    rows.append(f"## {el.summary.name}")
                else:
                    rows.append(self._render_header(el.summary.name))
            if el.summary.desc:
                rows.append(sanitize(el.summary.desc, allow_markdown=True))

        for item in el.contents:
            rows.append(self.render(item, is_flat=is_flat))

        return "\n\n".join(rows)

    @dispatch
    def render(self, el: layout.Doc) -> str:  # noqa: F811
        raise NotImplementedError(f"Unsupported Doc type: {type(el)}")

    @dispatch
    def render(  # noqa: F811
        self, el: Union[layout.DocClass, layout.DocModule], is_flat: bool = False
    ) -> str:
        title = "" if is_flat else self._render_header(el.name)

        sig = self.signature(el)
        body_rows = self.render(el.obj).split("\n")

        if el.members:
            # add attributes
            # skip if docstring has an attributes section
            raw_attrs = [x for x in el.members if x.obj.is_attribute]
            if raw_attrs and not _has_attr_section(el.obj.docstring):
                attr_rows = map(self.render, raw_attrs)
                attr_text = self._render_definition_list(
                    "Attributes:", attr_rows, title_class="highlight"
                )
                body_rows.extend(attr_text.split("\n"))

            # add classes
            for raw_class in el.members:
                if raw_class.obj.is_class and isinstance(raw_class, layout.Doc):
                    body_rows.extend(self.render(raw_class, is_flat=True).split("\n"))

            # add methods
            for raw_method in el.members:
                if raw_method.obj.is_function and isinstance(raw_method, layout.Doc):
                    body_rows.extend(self.render(raw_method, is_flat=True).split("\n"))

        text = self._render_definition_list(sig, body_rows)

        return "\n\n".join([title, text])

    @dispatch
    def render(  # noqa: F811
        self, el: layout.DocFunction, is_flat: bool = False
    ) -> str:
        title = "" if is_flat else self._render_header(el.name)

        sig = self.signature(el)
        body_rows = self.render(el.obj).split("\n")
        text = self._render_definition_list(sig, body_rows)

        return "\n\n".join([title, text])

    @dispatch
    def render(  # noqa: F811
        self, el: layout.DocAttribute, is_flat: bool = False
    ) -> str:
        link = f"[{el.name}](#{el.anchor})"
        description = self.summarizer.summarize(el.obj)

        # check for alias like "IntStr: TypeAlias = Optional[Union[int, str]]"
        if isinstance(el.obj, dc.Alias) and el.obj.target and el.obj.target.value:
            alias = f"alias of {self.render_annotation(el.obj.target)}"
            return self._render_definition_list(title=link, items=[description, alias])

        return " -- ".join([link, description])

    # render griffe objects ===================================================

    @dispatch
    def render(self, el: Union[dc.Object, dc.Alias]) -> str:  # noqa: F811
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
    def render(self, el: dc.Parameters) -> str:  # noqa: F811
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
    def render(self, el: dc.Parameter) -> str:  # noqa: F811
        splats = {dc.ParameterKind.var_keyword, dc.ParameterKind.var_positional}
        has_default = el.default and el.kind not in splats

        if el.kind == dc.ParameterKind.var_keyword:
            glob = "**"
        elif el.kind == dc.ParameterKind.var_positional:
            glob = "*"
        else:
            glob = ""

        name = sanitize(el.name)

        if has_default:
            res = f"{glob}{name}={el.default}"
        else:
            res = f"{glob}{name}"
        return res

    # docstring parts -------------------------------------------------------------

    # text ----
    # note this can be a number of things. for example, opening docstring text,
    # or a section with a header not included in the numpydoc standard
    @dispatch
    def render(self, el: ds.DocstringSectionText) -> str:  # noqa: F811
        new_el = qast.transform(el)
        if isinstance(new_el, ds.DocstringSectionText):
            # ensures we don't recurse forever
            return el.value

        return self.render(new_el)

    # parameters ----

    @dispatch
    def render(self, el: ds.DocstringSectionParameters) -> str:  # noqa: F811
        # if more than one param, render as un-ordered list
        prefix = "* " if len(el.value) > 1 else ""
        follow = "  " if len(el.value) > 1 else ""

        rows = []
        for param in el.value:
            name = sanitize(param.name)
            anno = self.render_annotation(param.annotation)
            default = f", default: {escape(param.default)}" if param.default else ""

            rows.append(f"{prefix}**{name}** ({anno}{default})")
            rows.append("")
            for row in param.description.split("\n"):
                rows.append(f"{follow}{row}")
            rows.append("")

        return self._render_definition_list(
            "Parameters:", rows, title_class="highlight"
        )

    # attributes ----

    @dispatch
    def render(self, el: ds.DocstringSectionAttributes) -> str:  # noqa: F811
        # if more than one param, render as un-ordered list
        prefix = "* " if len(el.value) > 1 else ""
        follow = "  " if len(el.value) > 1 else ""

        rows = []
        for attr in el.value:
            name = sanitize(attr.name)
            anno = self.render_annotation(attr.annotation)
            rows.append(f"{prefix}**{name}** ({anno})")
            rows.append("")
            for row in attr.description.split("\n"):
                rows.append(f"{follow}{row}")
            rows.append("")

        return self._render_definition_list(
            "Attributes:", rows, title_class="highlight"
        )

    # examples ----

    @dispatch
    def render(self, el: ds.DocstringSectionExamples) -> str:  # noqa: F811
        # its value is a tuple: DocstringSectionKind["text" | "examples"], str
        data = map(qast.transform, el.value)
        return "\n\n".join(list(map(self.render, data)))

    @dispatch
    def render(self, el: qast.ExampleCode):  # noqa: F811
        return f"""```python
{el.value}
```"""

    @dispatch
    def render(self, el: qast.ExampleText):  # noqa: F811
        return el.value

    # returns ----

    @dispatch
    def render(self, el: ds.DocstringSectionReturns) -> str:  # noqa: F811
        # if more than one param, render as un-ordered list
        prefix = "* " if len(el.value) > 1 else ""
        follow = "  " if len(el.value) > 1 else ""

        rows = []
        for item in el.value:
            title = prefix
            name = sanitize(item.name)
            if name:
                title += f"**{name}**"

            return_type = self.render_annotation(item.annotation)
            if return_type:
                title += return_type

            if title != prefix:
                rows.append(title)

            if item.description:
                rows.append("")
                for row in item.description.split("\n"):
                    rows.append(f"{follow}{row}")
                rows.append("")

        return self._render_definition_list("Returns:", rows, title_class="highlight")

    @dispatch
    def render(self, el: ds.DocstringSectionRaises) -> str:  # noqa: F811
        # if more than one param, render as un-ordered list
        prefix = "* " if len(el.value) > 1 else ""
        follow = "  " if len(el.value) > 1 else ""

        rows = []
        for item in el.value:
            # name = sanitize(item.name)
            anno = self.render_annotation(item.annotation)
            rows.append(f"{prefix}{anno}")
            rows.append("")
            for row in item.description.split("\n"):
                rows.append(f"{follow}{row}")
            rows.append("")

        return self._render_definition_list("Raises:", rows, title_class="highlight")

    @dispatch
    def render(self, el: ds.DocstringSectionAdmonition) -> str:  # noqa: F811
        rows = []
        if el.title.lower().startswith("note"):
            rows.append(f"::: {{.callout-note title={el.title!r}}}")
        elif el.title.lower().startswith("warn"):
            rows.append(f"::: {{.callout-warning title={el.title!r}}}")
        else:
            rows.append(f"::: {{.callout-tip title={el.title!r}}}")

        rows.append(sanitize(el.value.description, allow_markdown=True))
        rows.append(":::")

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
    def render(self, el):  # noqa: F811
        raise NotImplementedError(f"{type(el)}")
