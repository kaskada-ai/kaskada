
# Kaskada: Modern, open-source event-processing

<p align="center">
  <a href="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_proto.yml">
    <img src="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_proto.yml/badge.svg" alt="Protobuf CI" style="max-width: 100%;">
  </a>
  <a href="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_engine.yml">
    <img src="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_engine.yml/badge.svg" alt="Engine CI" style="max-width: 100%;">
  </a>
  <a href="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_with_rust_nightly.yml">
    <img src="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_with_rust_nightly.yml/badge.svg" alt="Rust CI (Nightly)" style="max-width: 100%;">
  </a>
  <a href="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_client_python.yml">
    <img src="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_client_python.yml/badge.svg" alt="Python Client CI" style="max-width: 100%;">
  </a>
  <a href="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_notebooks.yml">
    <img src="https://github.com/kaskada-ai/kaskada/actions/workflows/ci_notebooks.yml/badge.svg" alt="Notebooks CI" style="max-width: 100%;">
  </a>
</p>

<p align="center">
  <a href="https://kaskada.io">kaskada.io</a>
  |
  <a href="https://kaskada.io/docs-site/">Docs</a>
</p>

Kaskada is a unified event processing engine that provides all the power of stateful stream processing in a high-level, declarative query language designed specifically for reasoning about events in bulk and in real time.

Kaskada's query language builds on the best features of SQL to provide a more expressive way to compute over events. Queries are simple and declarative. Unlike SQL, they are also concise, composable, and designed for processing events. By focusing on the event-processing use case, Kaskada's query language makes it easier to reason about when things happen, state at specific points in time, and how results change over time.

Kaskada is implemented as a modern compute engine designed for processing events in bulk or real-time. Written in Rust and built on Apache Arrow, Kaskada can compute most workloads without the complexity and overhead of distributed execution.

Read more at [kaskada.io](https://kaskada.io).
See the [docs](https://kaskada.io/docs-site/) to get started in a [Jupyter Notebook](https://kaskada.io/docs-site/kaskada/main/getting-started/hello-world-jupyter.html) or the [CLI](https://kaskada.io/docs-site/kaskada/main/getting-started/hello-world-cli.html).

## Features

- **Stateful aggregations**: Aggregate events to produce a continuous timeline whose value can be observed at arbitrary points in time.
- **Automatic joins**: Every expression is associated with an “entity”, allowing tables and expressions to be automatically joined. Entities eliminate redundant boilerplate code.
- **Event-based windowing**: Collect events as you move through time, and aggregate them with respect to other events. Ordered aggregation makes it easy to describe temporal interactions.
- **Pipelined operations**: Pipe syntax allows multiple operations to be chained together. Write your operations in the same order you think about them. It's timelines all the way down, making it easy to aggregate the results of aggregations.
- **Row generators**: Pivot from events to time-series. Unlike grouped aggregates, generators produce rows even when there's no input, allowing you to react when something doesn't happen.
- **Continuous expressions**: Observe the value of aggregations at arbitrary points in time. Timelines are either “discrete” (instantaneous values or events) or “continuous” (values produced by a stateful aggregations). Continuous timelines let you combine aggregates computed from different event sources.
- **Native time travel**: Shift values forward (but not backward) in time, allowing you to combine different temporal contexts without the risk of temporal leakage. Shifted values make it easy to compare a value “now” to a value from the past.
- **Simple, composable syntax**: It is functions all the way down. No global state, no dependencies to manage, and no spooky action at a distance. Quickly understand what a query is doing, and painlessly refactor to make it DRY.

## Join Us!
We're building an active, inclusive community of users and contributors. 
Come get to know us on [Slack](https://join.slack.com/t/kaskada-hq/shared_invite/zt-1t1lms085-bqs2jtGO2TYr9kuuam~c9w) - we'd love to meet you!

For specific problems, file an [issue](https://github.com/kaskada-ai/kaskada/issues).

## Discussion and Development
Most development discussions take place on GitHub in this repo.

## Contributing
All contributions -- issues, fixes, documentation improvements, features and ideas -- are welcome.

See [CONTRIBUTING.md](CONTRIBUTING.md) for more details.


