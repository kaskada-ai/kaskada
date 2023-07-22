# Python Library for Kaskada

`sparrow-py` provides a Python library for building and executing Kaskada queries using an embedded Sparrow library.
It uses [Pyo3][pyo3] to generate Python wrappers for the Rust code, and then provides more Python-friendly implementations on top of that.

[pyo3]: https://github.com/PyO3/pyo3

## Install Python

Use `pyenv` and install at least `3.7` or `3.8`.
If multiple versions are installed, `nox` will test against each of them.

## Building and Testing

To build this package, first install `maturin`:

```shell
poetry shell
poetry install
maturin develop
pytest
```

Alternatively, install nox and run the tests inside an isolated environment:

```shell
nox
```
