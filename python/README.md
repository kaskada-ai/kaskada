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
