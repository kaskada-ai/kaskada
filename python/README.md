# Kaskada Timestreams

<!-- start elevator-pitch -->
Kaskada's `timestreams` library makes it easy to work with structured event-based data.
Define temporal queries on event-based data loaded from Python, using Pandas or PyArrow and push new data in as it occurs.
Or, execute the queries directly on events in your data lake and/or as they arrive on a stream.

With Kaskada you can unleash the value of real-time, temporal queries without the complexity of "big" infrastructure components like a distributed stream or stream processing system.

Under the hood, `timestreams` is an efficient temporal query engine built in Rust.
It is built on Apache Arrow, using the same columnar execution strategy that makes ...

<!-- end elevator-pitch -->

## Install Python

Use `pyenv` and install at least `3.8` (most development occurs under `3.11`).
If multiple versions are installed, `nox` will test against each of them.

## Building and Testing

To build this package, first install `maturin`:

```shell
poetry shell
poetry install --no-root
maturin develop
pytest
```

Alternatively, install nox and run the tests inside an isolated environment:

```shell
nox
```


## Previewing Docs

* Install `quarto-cli` on your machine. Also consider installing an IDE extension.

  See: https://quarto.org/docs/get-started/

* Generate reference docs

  ```shell
  nox -s docs-gen
  ```

  You should re-run this after making any updates to the `pysrc` docstrings.
  If _Preview Docs_ is running in another shell, the system should auto-refresh with your changes.

* Preview docs (with auto-refresh on edit)

  ```shell
  nox -s docs
  ```

* Cleanup generated and cached docs

  ```shell
  nox -s docs-clean
  ```

  Try this if you see something unexpected (especially after deleting or renaming).

* Builds docs to `docs/_site`

  ```shell
  nox -s docs-build
  ```

  This is primarily used in CI.
