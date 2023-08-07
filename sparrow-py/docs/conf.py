"""Sphinx configuration."""
from typing import Any
from typing import Dict


project = "sparrow-py"
author = "Kaskada Contributors"
copyright = "2023, Kaskada Contributors"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
html_title = "Kaskada"
language = "en"

intersphinx_mapping: Dict[str, Any] = {
    "python": ("http://docs.python.org/3", None),
    "pandas": ("http://pandas.pydata.org/docs", None),
    "pyarrow": ("https://arrow.apache.org/docs", None),
}

html_theme_options: Dict[str, Any] = {
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/kaskada-ai/kaskada",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16">
                    <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path>
                </svg>
            """,
            "class": "",
        },
    ],
    "source_repository": "https://github.com/kaskada-ai/kaskada/",
    "source_branch": "main",
    "source_directory": "sparrow-py/docs/",
}

# Options for Todos
todo_include_todos = True

# Options for Myst (markdown)
# https://myst-parser.readthedocs.io/en/v0.17.1/syntax/optional.html
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "smartquotes",
    "replacements",
]
myst_heading_anchors = 3

# Suggested options from Furo theme
# -- Options for autodoc ----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

# Automatically extract typehints when specified and place them in
# descriptions of the relevant function/method.
autodoc_typehints = "description"

# Don't show class signature with the class' name.
autodoc_class_signature = "separated"

autodoc_type_aliases = {"Arg": "sparrow_py.Arg"}
