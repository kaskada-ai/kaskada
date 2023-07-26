"""Sphinx configuration."""
project = "sparrow-py"
author = "Kaskada Contributors"
copyright = "2023, Kaskada Contributors"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
