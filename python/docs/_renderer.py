from __future__ import annotations

import quartodoc.ast as qast

from contextlib import contextmanager
from functools import wraps
from griffe.docstrings import dataclasses as ds
from griffe import dataclasses as dc
from tabulate import tabulate
from plum import dispatch
from typing import Tuple, Union, Optional
from quartodoc import layout

from _base import BaseRenderer, escape, sanitize, convert_rst_link_to_md


try:
    # Name and Expression were moved to expressions in v0.28
    from griffe import expressions as expr
except ImportError:
    from griffe import dataclasses as expr


def _has_attr_section(el: dc.Docstring | None):
    if el is None:
        return False

    return any([isinstance(x, ds.DocstringSectionAttributes) for x in el.parsed])


class Renderer(BaseRenderer):
    """Render docstrings to markdown.

    Parameters
    ----------
    header_level: int
        The level of the header (e.g. 1 is the biggest).
    show_signature: bool
        Whether to show the function signature.
    show_signature_annotations: bool
        Whether to show annotations in the function signature.
    display_name: str
        The default name shown for documented functions. Either "name", "relative",
        "full", or "canonical". These options range from just the function name, to its
        full path relative to its package, to including the package name, to its
        the its full path relative to its .__module__.

    Examples
    --------

    >>> from quartodoc import MdRenderer, get_object
    >>> renderer = MdRenderer(header_level=2)
    >>> f = get_object("quartodoc", "get_object")
    >>> print(renderer.render(f)[:81])
    ## get_object
    `get_object(module: str, object_name: str, parser: str = 'numpy')`

    """

    style = "markdown"

    def __init__(
        self,
        header_level: int = 1,
        show_signature: bool = True,
        show_signature_annotations: bool = False,
        display_name: str = "relative",
        hook_pre=None,
        render_interlinks=False,
    ):
        self.header_level = header_level
        self.show_signature = show_signature
        self.show_signature_annotations = show_signature_annotations
        self.display_name = display_name
        self.hook_pre = hook_pre
        self.render_interlinks = render_interlinks

        self.crnt_header_level = self.header_level

    @contextmanager
    def _increment_header(self, n=1):
        self.crnt_header_level += n
        try:
            yield
        finally:
            self.crnt_header_level -= n

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

    def _render_definition_list(self, title: str, items: [str]) -> str:
        rows = [title]
        for item in items:
            if len(rows) == 1:
                rows.append(f'~   {item}')
            else:
                rows.append(f'    {item}')
        return "\n".join(rows)

    def _render_header(self, title: str, order: Optional[str] = None) -> str:
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
        return f"[{sanitize(el.source)}](`{el.full}`)"

    @dispatch
    def render_annotation(self, el: expr.Expression) -> str:
        return "".join(map(self.render_annotation, el))

    # signature method --------------------------------------------------------

    @dispatch
    def signature(self, el: layout.Doc):
        orig = self.display_name

        # set signature path, generate signature, then set back
        self.display_name = el.signature_name
        res = self.signature(el.obj)
        self.display_name = orig

        return res

    @dispatch
    def signature(self, el: dc.Alias, source: Optional[dc.Alias] = None):
        """Return a string representation of an object's signature."""
        return self.signature(el.target, el)

    @dispatch
    def signature(self, el: dc.Function, source: Optional[dc.Alias] = None) -> str:
        name = self._fetch_object_dispname(source or el)
        pars = self.render(self._fetch_method_parameters(el))
        return f"{name}(*** {pars} ***)"

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
    def render(self, el: Union[layout.DocClass, layout.DocModule], single_page: bool = False):
        title = self._render_header(el.name)

        attr_docs = []
        meth_docs = []
        class_docs = []

        if el.members:
            sub_header = "#" * (self.crnt_header_level + 1)
            raw_attrs = [x for x in el.members if x.obj.is_attribute]
            raw_meths = [x for x in el.members if x.obj.is_function]
            raw_classes = [x for x in el.members if x.obj.is_class]

            header = "| Name | Description |\n| --- | --- |"

            # attribute summary table ----
            # docstrings can define an attributes section. If that exists on
            # then we skip summarizing each attribute into a table.
            # TODO: for now, we skip making an attribute table on classes, unless
            # they contain an attributes section in the docstring
            if (
                raw_attrs
                and not _has_attr_section(el.obj.docstring)
                # TODO: what should backwards compat be?
                # and not isinstance(el, layout.DocClass)
            ):

                _attrs_table = "\n".join(map(self.summarize, raw_attrs))
                attrs = f"{sub_header} Attributes\n\n{header}\n{_attrs_table}"
                attr_docs.append(attrs)

            # classes summary table ----
            if raw_classes:
                _summary_table = "\n".join(map(self.summarize, raw_classes))
                section_name = "Classes"
                objs = f"{sub_header} {section_name}\n\n{header}\n{_summary_table}"
                class_docs.append(objs)

                n_incr = 1 if el.flat else 2
                with self._increment_header(n_incr):
                    class_docs.extend(
                        [
                            self.render(x)
                            for x in raw_classes
                            if isinstance(x, layout.Doc)
                        ]
                    )

            # method summary table ----
            if raw_meths:
                #     _summary_table = "\n".join(map(self.summarize, raw_meths))
                # section_name = (
                #     "Methods" if isinstance(el, layout.DocClass) else "Functions"
                # )
                # objs = f"{sub_header} {section_name}\n\n{header}\n{_summary_table}"
                # meth_docs.append(objs)

                # TODO use context manager, or context variable?
                n_incr = 1 if el.flat else 2
                with self._increment_header(n_incr):
                    meth_docs.extend(
                        [self.render(x, single_page=True)
                         for x in raw_meths if isinstance(x, layout.Doc)]
                    )

        sig = self.signature(el)
        body_rows = self.render(el.obj).split("\n")
        text = self._render_definition_list(sig, body_rows)

        return "\n\n".join([title, text, *attr_docs, *class_docs, *meth_docs])

    @dispatch
    def render(self, el: Union[layout.DocFunction, layout.DocAttribute], single_page: bool = False):
        title = "" if single_page else self._render_header(el.name)

        sig = self.signature(el)
        body_rows = self.render(el.obj).split("\n")
        text = self._render_definition_list(sig, body_rows)

        return "\n\n".join([title, text])

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
        # TODO: missing annotation
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

        rows = []
        for param in el.value:
            anno = self.render_annotation(param.annotation)
            desc = sanitize(param.description, allow_markdown=True)
            default = f', default: {escape(param.default)}' if param.default else ""

            rows.append(f'{prefix}**{param.name}** ({anno}{default}) -- {desc}')

        return self._render_definition_list("Parameters:", rows)

    # attributes ----

    @dispatch
    def render(self, el: ds.DocstringSectionAttributes):
        header = ["Name", "Type", "Description"]
        rows = list(map(self.render, el.value))
        return self._render_table(rows, header)

    @dispatch
    def render(self, el: ds.DocstringAttribute):
        row = [
            sanitize(el.name),
            self.render_annotation(el.annotation),
            sanitize(el.description or "", allow_markdown=True),
        ]
        return row

    # warnings ----

    @dispatch
    def render(self, el: qast.DocstringSectionWarnings):
        return el.value

    # see also ----

    @dispatch
    def render(self, el: qast.DocstringSectionSeeAlso):
        # TODO: attempt to parse See Also sections
        return convert_rst_link_to_md(el.value)

    # notes ----

    @dispatch
    def render(self, el: qast.DocstringSectionNotes):
        return el.value

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
    def render(self, el: Union[ds.DocstringSectionReturns, ds.DocstringSectionRaises]):
        # render as a definition list
        rows = list(map(self.render, el.value))
        return "\n".join(rows)

    @dispatch
    def render(self, el: ds.DocstringReturn) -> str:
        returns = []
        return_type = self.render_annotation(el.annotation)
        if return_type:
            returns.append(return_type)

        return_desc = sanitize(el.description, allow_markdown=True)
        if return_desc:
            returns.append(return_desc)

        returns_text = " -- ".join(returns)
        if returns_text:
            return self._render_definition_list("Returns:", [returns_text])
        else:
            return ""

    @dispatch
    def render(self, el: ds.DocstringRaise):
        # similar to DocstringParameter, but no name or default
        annotation = self.render_annotation(el.annotation)
        return f'{annotation} -- {sanitize(el.description, allow_markdown=True)}'

    @dispatch
    def render(self, el: ds.DocstringSectionAdmonition):
        return sanitize(el.value.description, allow_markdown=True)

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
    # this method returns a summary description, such as a table summarizing a
    # layout.Section, or a row in the table for layout.Page or layout.DocFunction.

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
