"""Sphinx configuration."""
import sys
from pathlib import Path
from typing import Any, Dict


sys.path.append(str(Path(".").resolve()))

project = "kaskada"
author = "Kaskada Contributors"
copyright = "2023, Kaskada Contributors"
extensions = [
    "ablog",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx_design",
    # "myst_parser",
    "myst_nb",
    "sphinx_copybutton",
    "sphinx_autodoc_typehints",  # must be after napoleon
    "sphinx_social_cards",
    "_extensions.gallery_directive",
]
autodoc_typehints = "description"
language = "en"

html_theme = "pydata_sphinx_theme"
html_favicon = "_static/favicon.png"
html_title = "Kaskada"
html_js_files = [
    "https://cdnjs.cloudflare.com/ajax/libs/require.js/2.3.4/require.min.js"
]

# Configure the primary (left) sidebar.
# https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/layout.html#primary-sidebar-left
html_sidebars = {
    # No primary (left) sidebar on the landing page.
    "index.md": [],
    # Blog sidebars
    # https://ablog.readthedocs.io/manual/ablog-configuration-options/#blog-sidebars
    "blog/**": [
        # Information about the post.
        "ablog/postcard.html",
        # 5 most recent posts
        "ablog/recentposts.html",
        # Tag cloud and links.
        "ablog/tagcloud.html",
        # Categories -- we just use tags for now.
        # "ablog/categories.html",
        # Show all authors on the sidebar.
        # "ablog/authors.html",
        # Show all languages on the sidebar.
        # "ablog/languages.html",
        # Show all locations on the sidebar.
        # "ablog/locations.html",
        "ablog/archives.html",
    ],
    "[!blog]*/*": ["sidebar-nav-bs"],
}

html_theme_options: Dict[str, Any] = {
    # Setup external links.
    # https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/header-links.html
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/kaskada-ai/kaskada",  # required
            "icon": "fa-brands fa-square-github",
            "type": "fontawesome",
        },
        {
            "name": "Slack",
            "url": "https://join.slack.com/t/kaskada-hq/shared_invite/zt-1t1lms085-bqs2jtGO2TYr9kuuam~c9w",
            "icon": "fa-brands fa-slack",
        },
    ],
    # Setup edit buttons
    # See https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/source-buttons.html
    "use_edit_page_button": True,
    # Include indices.
    # https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/indices.html
    "primary_sidebar_end": ["indices.html"],
    # Provide an announcement at the top.
    # https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/announcements.html#
    "announcement": (
        "This describes the next version of Kaskada. "
        "It is currently available as an alpha release."
    ),
    # Branding / logos.
    # https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/branding.html
    "logo": {
        "image_light": "_static/kaskada-positive.svg",
        "image_dark": "_static/kaskada-negative.svg",
    },
    # Setup analytics.
    # https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/analytics.html
    "analytics": {
        "google_analytics_id": "G-HR9E2E6TG4",
    },
    # TODO: Version switcher.
    # This would require hosting multiple versions of the docs.
    # https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/version-dropdown.html
}

templates_path = ["_templates"]
html_static_path = ["_static"]

html_context = {
    "github_user": "kaskada-ai",
    "github_repo": "kaskada",
    "github_version": "main",
    "doc_path": "python/docs/source",
}

# Setup links to other sphinx projects.
intersphinx_mapping: Dict[str, Any] = {
    "python": ("http://docs.python.org/3", None),
    "pandas": ("http://pandas.pydata.org/docs", None),
    "pyarrow": ("https://arrow.apache.org/docs", None),
}

# adds useful copy functionality to all the examples; also
# strips the '>>>' and '...' prompt/continuation prefixes.
copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True

# Options for Todos
todo_include_todos = True

# Options for Myst (markdown)
# https://myst-parser.readthedocs.io/en/v0.17.1/syntax/optional.html
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "smartquotes",
    "replacements",
    "linkify",
]
myst_heading_anchors = 3

# -- Options for autodoc ----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

# Automatically extract typehints when specified and place them in
# descriptions of the relevant function/method.
autodoc_typehints = "description"
autodoc_type_aliases = {"kaskada.Arg": "kaskada.Arg"}

# Don't show class signature with the class' name.
autodoc_class_signature = "separated"

autosummary_generate = True

napoleon_preprocess_types = True
napoleon_attr_annotations = True
napoleon_use_rtype = False
typehints_use_rtype = False
typehints_document_rtype = False
typehints_defaults = "comma"

suppress_warnings = ["mystnb.unknown_mime_type"]

blog_path = "blog"
blog_authors = {
    "ben": ("Ben Chambers", "https://github.com/bjchambers"),
    "ryan": ("Ryan Michael", "https://github.com/kerinin"),
}
post_date_format = "%Y-%b-%d"
post_date_format_short = "%Y-%b-%d"
post_show_prev_next = False

# Generate social cards for blog posts
social_cards = {
    "site_url": "https://kaskada.io/kaskada",
    "description": "Kaskada: Real-Time AI without the fuss.",
    "cards_layout_dir": ["_layouts"],
    "cards_layout_options": {
        "background_color": "#26364a",
    },
}
