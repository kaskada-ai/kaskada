
# Project

[![Protobuf CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_proto.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_proto.yml)
[![Rust CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_rust.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_rust.yml)
[![Rust CI (Nightly)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_with_rust_nightly.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_with_rust_nightly.yml)
[![Wren CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_wren.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_wren.yml)
[![Python Client CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_client_python.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_client_python.yml)
[![Notebooks CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_notebooks.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_notebooks.yml)

Kaskada is a unified event processing engine that provides the power of stateful stream processing in a high-level, declarative query language designed specifically for reasoning about events in bulk and in real time.

Existing event-processing tools like Spark Streaming or Apache Flink give you multiple APIs - you can choose between a powerful low-level API or a convenient high-level query language like stream-SQL. The high-level APIs are great for usability, but a lot is lost in the translation from streams to tables (this is why they provide both).

Kaskada provides a single, high-level, declarative API. Instead of being based on tables, it’s built on the idea of a timeline - the history of how a value changes over time. This starting point eliminates the tradeoff between expressibility and convenience. You get the declarative transformations and aggregations of SQL without losing the ability to reason about temporal context, time travel, sequencing, timeseries, etc.

Kaskada is built to be a modern event processor - it’s efficient, cloud-native, and easy to use. It uses the Apache Arrow library for high-performance columnar computation. It’s implemented in Rust, which gives you safety and the stability of not having a garbage collector. It’s distributed as a compiled binary without any dependencies to manage - just download it and you’re ready to go. 

Kaskada isn’t ideal for every use case - it’s designed specifically for event processing so it isn’t appropriate for graph data or non-temporal relational data. But for the case of event processing, both in bulk and in real-time, we believe that Kaskada is the best option available right now.


## Development

### Setup Environment
#### Mac OS
* Install [LLVM/Clang](https://clang.llvm.org/get_started.html) via the XCode tools.
* Install protoc: `brew install protobuf`
* Install golang: `brew install golang`
* Install [Docker](https://docs.docker.com/desktop/install/mac-install/)
* Increase open file limit: `echo 'ulimit -n 4096' >> ~/.zshrc`
* Install Python (3.8.16) via [pyenv](https://github.com/pyenv/pyenv)
  * `brew install pyenv`
  * `pyenv install 3.8.16`

#### Linux (Debian-based)
* `apt install clang lld libssl-dev pkg-config protobuf-compiler`
* Install [Docker](https://docs.docker.com/engine/install/ubuntu/)
* Increase open file limit: `echo 'ulimit -n 4096' >> ~/.bashrc`
* Install [Python 3.8.16](https://www.python.org/downloads/release/python-3816/)

#### All platforms: install Rust
* Install Rust using [rustup](https://www.rust-lang.org/tools/install).
* Install the following Rustup components
    * `rustup component add rust-src` -- Rust source code for IDE completion.
    * `rustup component add clippy rustfmt` -- Rust linter and formatter.

### Testing & Building the Compute Engine
Running `cargo test` will run all the tests for the compute engine.

Run `cargo build --release -p sparrow-main` to build a release (optimized) binary of the main executable.

### Testing & Building the API

* ensure docker is running locally
* run `make proto/generate` and `make ent/generate`.  See the `./wren/README.md` for more info on those.
* run `make wren/test`

### Testing & Building the Python Client

* Verify that Python 3.8.16 is installed locally (other versions may be compatible too): `python --version`
* Install Poetry: `pip install poetry`
* Run `make python/setup` to install the dependencies with poetry.
* Run `make python/test` to run the tests.
* Run `make python/build` to build the wheel.
* Run `make python/install` to build the wheel and install it locally.

### Configurations
* `TMPDIR` - The compute engine uses temporary files as part of computation. By default, this uses the default temporary file directory (platform dependent). See: [tempfile::NamedTempFile](https://docs.rs/tempfile/1.1.2/tempfile/struct.NamedTempFile.html). To set the temporary path directory, set the `TMPDIR` environment variable. See: [std::env::temp_dir](https://doc.rust-lang.org/std/env/fn.temp_dir.html).

### Running integration tests

* run `make test/int/docker-up` in one terminal window to get the Kaskada service and dependencies up
* run `make test/int/run-api` in another terminal window to run the manager integration tests

After making code changes, `ctrl-c` in the services window and restart it.

**Note:** that there are also other make commands to test the other supported object stores and databases. Append any of the following on your make commands to test other scenarios:
* `-s3`: s3 object storage (minio) with sqlite db
* `-postgres`: local object storage with postgres db
* `-postgres-s3`: s3 object storage (minio) with postgres db

## Visual Studio Code

* Install [Visual Studio Code (VSC)](https://code.visualstudio.com/download)
* Install the following VSC extensions
*  * [Rust Analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer). Essential.
    Provides the language server integration for Rust code.
*  * [Even Better TOML](https://marketplace.visualstudio.com/items?itemName=tamasfe.even-better-toml). Optional.
*  * [Cargo](https://marketplace.visualstudio.com/items?itemName=panicbit.cargo). Optional.
*  * [Crates](https://marketplace.visualstudio.com/items?itemName=serayuzgur.crates) Optional.

## Links

* [Kaskada Docs](https://kaskada-ai.github.io/docs-site) for more information.
