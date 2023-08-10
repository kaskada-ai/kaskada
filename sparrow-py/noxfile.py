"""Nox sessions."""
import os
import shlex
import shutil
import sys
from pathlib import Path
from textwrap import dedent

import nox

try:
    from nox_poetry import Session, session
except ImportError:
    message = f"""\
    Nox failed to import the 'nox-poetry' package.

    Please install it using the following command:

    {sys.executable} -m pip install nox-poetry"""
    raise SystemExit(dedent(message)) from None


package = "sparrow_py"
python_versions = ["3.11", "3.10", "3.9", "3.8"]
nox.needs_version = ">= 2021.6.6"
nox.options.sessions = (
    "check-lint",
    "safety",
    "mypy",
    "tests",
    "typeguard",
    "xdoctest",
    "docs-build",
)


def install_self(session: Session) -> None:
    """Install the sparrow-py crate locally using `maturin deveop`."""
    session.install("maturin")
    session.run_always("maturin", "develop")


@session(name="check-lint", python=python_versions[0])
def check_lint(session: Session) -> None:
    """Lint."""
    args = session.posargs or ["pysrc", "pytests", "docs/source"]
    session.install(
        "black",
        "darglint",
        "flake8",
        "flake8-bugbear",
        "flake8-rst-docstrings",
        "isort",
        "pep8-naming",
        "pydocstyle",
        "pyupgrade",
    )
    session.run
    session.run("black", "--check", *args)
    session.run("flake8", *args)
    session.run("isort", "--filter-files", "--check-only", *args)

    # Only do darglint and pydocstyle on pysrc (source)
    session.run("darglint", "pysrc")
    session.run("pydocstyle", "--convention=numpy", "pysrc")
    # No way to run this as a check.
    # session.run("pyupgrade", "--py38-plus")

@session(name="fix-lint", python=python_versions[0])
def fix_lint(session: Session) -> None:
    """Automatically fix lint issues."""
    args = session.posargs or ["pysrc", "pytests", "docs/source"]
    session.install(
        "black",
        "isort",
        "autoflake",
        "pep8-naming",
        "pyupgrade",
    )
    session.run
    session.run("black", *args)
    session.run("autoflake", "--in-place", "--remove-all-unused-imports", "--recursive", *args)
    session.run("isort", "--filter-files", *args)
    session.run("pyupgrade", "--py38-plus")


@session(python=python_versions[0])
def safety(session: Session) -> None:
    """Scan dependencies for insecure packages."""
    requirements = session.poetry.export_requirements()
    session.install("safety")
    session.run("safety", "check", "--full-report", f"--file={requirements}")


@session(python=python_versions)
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or ["pysrc", "pytests", "docs/source/conf.py"]
    session.install("mypy", "pytest", "pandas-stubs")
    install_self(session)
    # Using `--install-types` should make this less picky about missing stubs.
    # However, there is a possibility it slows things down, by making mypy
    # run twice -- once to determine what types need to be installed, then once
    # to check things with those stubs.
    session.run("mypy", "--install-types", "--non-interactive", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@session(python=python_versions)
def tests(session: Session) -> None:
    """Run the test suite."""
    session.install("coverage[toml]", "pytest", "pygments", "pandas", "pyarrow", "pytest-asyncio")
    install_self(session)
    try:
        session.run("coverage", "run", "--parallel", "-m", "pytest", *session.posargs)
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@session(python=python_versions[0])
def coverage(session: Session) -> None:
    """Produce the coverage report."""
    args = session.posargs or ["report"]

    session.install("coverage[toml]")

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@session(python=python_versions[0])
def typeguard(session: Session) -> None:
    """Runtime type checking using Typeguard."""
    install_self(session)
    session.install("pytest", "typeguard", "pygments", "pandas", "pyarrow")
    session.run("pytest", f"--typeguard-packages={package}", *session.posargs)


@session(python=python_versions)
def xdoctest(session: Session) -> None:
    """Run examples with xdoctest."""
    if session.posargs:
        args = [package, *session.posargs]
    else:
        args = [f"--modname={package}", "--command=all"]
        if "FORCE_COLOR" in os.environ:
            args.append("--colored=1")

    install_self(session)
    session.install("xdoctest[colors]")
    session.run("python", "-m", "xdoctest", *args)

DOCS_DEPS = [
    "myst-nb",
    "myst-parser",
    "pandas",
    "pyarrow",
    "sphinx-autobuild",
    "sphinx-book-theme",
    "sphinx-copybutton",
    "sphinx-design",
    "sphinx",
]

@session(name="docs-build", python=python_versions[0])
def docs_build(session: Session) -> None:
    """Build the documentation."""
    args = session.posargs or ["docs/source", "docs/_build", "-j", "auto"]
    if not session.posargs and "FORCE_COLOR" in os.environ:
        args.insert(0, "--color")

    install_self(session)
    session.install(*DOCS_DEPS)

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-build", *args)


@session(python=python_versions[0])
def docs(session: Session) -> None:
    """Build and serve the documentation with live reloading on file changes."""
    args = session.posargs or ["--open-browser", "docs/source", "docs/_build", "-j", "auto"]
    install_self(session)
    session.install(*DOCS_DEPS)

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-autobuild", *args)