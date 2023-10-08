"""Nox sessions."""
import os
import shutil
import sys
from pathlib import Path
from typing import Iterable
from typing import Iterator

import nox

package = "kaskada"
python_versions = ["3.11", "3.10", "3.9"]
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


@nox.session(name="check-lint", python=python_versions[0])
def check_lint(session: nox.Session) -> None:
    """Lint."""
    args = session.posargs or ["pysrc", "pytests", "docs/source"]
    install(session, groups=["lint"], root=False)
    session.run("black", "--check", *args)
    session.run("flake8", *args)
    session.run("isort", "--filter-files", "--check-only", *args)

    session.run("pydocstyle", "--convention=google", "pysrc")
    # No way to run this as a check.
    # session.run("pyupgrade", "--py38-plus")


@nox.session(name="fix-lint", python=python_versions[0])
def fix_lint(session: nox.Session) -> None:
    """Automatically fix lint issues."""
    args = session.posargs or ["pysrc", "pytests", "docs/source"]
    install(session, groups=["lint"], root=False)
    session.run("autoflake", "--in-place", "--remove-all-unused-imports", "--recursive", *args)
    session.run("isort", "--filter-files", *args)
    session.run("pyupgrade", "--py38-plus")
    session.run("black", *args)


@nox.session(python=python_versions[0])
def safety(session: nox.Session) -> None:
    """Scan dependencies for insecure packages."""
    # NOTE: Pass `extras` to `export_requirements` if the project supports any.
    requirements = export_requirements(session)
    install(session, groups=["safety"], root=False)
    session.run("safety", "check", "--full-report", f"--file={requirements}")


@nox.session(python=python_versions)
def mypy(session: nox.Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or ["pysrc", "pytests"]
    install(session, groups=["typecheck"])
    # Using `--install-types` should make this less picky about missing stubs.
    # However, there is a possibility it slows things down, by making mypy
    # run twice -- once to determine what types need to be installed, then once
    # to check things with those stubs.
    session.run("mypy", "--install-types", "--non-interactive", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@nox.session(python=python_versions)
def tests(session: nox.Session) -> None:
    """Run the test suite."""
    install(session, groups=["test"])
    try:
        session.run("coverage", "run", "--parallel", "-m", "pytest", *session.posargs)
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@nox.session(python=python_versions[0])
def coverage(session: nox.Session) -> None:
    """Produce the coverage report."""
    args = session.posargs or ["report"]

    install(session, groups=["test"])

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@nox.session(python=python_versions[0])
def typeguard(session: nox.Session) -> None:
    """Runtime type checking using Typeguard."""
    install(session, groups=["typecheck", "test"])
    session.run("pytest", f"--typeguard-packages={package}", *session.posargs)


@nox.session(python=python_versions)
def xdoctest(session: nox.Session) -> None:
    """Run examples with xdoctest."""
    if session.posargs:
        args = [package, *session.posargs]
    else:
        args = [f"--modname={package}", "--command=all"]
        if "FORCE_COLOR" in os.environ:
            args.append("--colored=1")

    install(session, groups=["test"])
    session.run("python", "-m", "xdoctest", *args)


@nox.session(name="docs-gen", python=python_versions[0])
def docs_gen(session: nox.Session) -> None:
    """Generate API reference docs"""
    install(session, groups=["docs"])

    reference_dir = Path("docs", "reference")
    if reference_dir.exists():
        shutil.rmtree(reference_dir)

    interlinks_dir = Path("docs", "_inv")
    if interlinks_dir.exists():
        shutil.rmtree(interlinks_dir)

    objects_file = Path("docs", "objects.json")
    if objects_file.exists():
        objects_file.unlink()

    with session.chdir("docs"):
        session.run("python", "_scripts/gen_reference.py")
        session.run("python", "-m", "quartodoc", "interlinks")


@nox.session(python=python_versions[0])
def docs(session: nox.Session) -> None:
    """Build and serve the documentation with live reloading on file changes."""

    install(session, groups=["docs"])

    build_dir = Path("docs", ".quarto", "_site")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    with session.chdir("docs"):
        session.run("quarto", "preview", external=True)


def install(session: nox.Session, *, groups: Iterable[str], root: bool = True) -> None:
    """Install the dependency groups using Poetry.
    This function installs the given dependency groups into the session's
    virtual environment. When ``root`` is true (the default), the function
    also installs the root package's default dependencies.

    The root package is installed using `maturin develop`.

    Args:
        session: The Session object.
        groups: The dependency groups to install.
        root: Install the root package.
    """
    session.run_always(
        "poetry",
        "install",
        "--no-root",
        "--sync",
        "--{}={}".format("only" if not root else "with", ",".join(groups)),
        external=True,
    )
    if root:
        session.run_always("maturin", "develop", "--profile", "dev")


def export_requirements(session: nox.Session, *, extras: Iterable[str] = ()) -> Path:
    """Export a requirements file from Poetry.
    This function uses ``poetry export`` to generate a requirements file
    containing the default dependencies at the versions specified in
    ``poetry.lock``.

    Args:
        session: The Session object.
        extras: Extras supported by the project.
    Returns:
        The path to the requirements file.
    """
    # XXX Use poetry-export-plugin with dependency groups
    output = session.run_always(
        "poetry",
        "export",
        "--format=requirements.txt",
        "--without-hashes",
        *[f"--extras={extra}" for extra in extras],
        external=True,
        silent=True,
        stderr=None,
    )

    if output is None:
        session.skip(
            "The command `poetry export` was not executed"
            " (a possible cause is specifying `--no-install`)"
        )

    assert isinstance(output, str)  # noqa: S101

    def _stripwarnings(lines: Iterable[str]) -> Iterator[str]:
        for line in lines:
            if line.startswith("Warning:"):
                print(line, file=sys.stderr)
                continue
            yield line

    text = "".join(_stripwarnings(output.splitlines(keepends=True)))

    path = session.cache_dir / "requirements.txt"
    path.write_text(text)

    return path
