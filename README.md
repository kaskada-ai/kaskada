
# Project

[![Protobuf CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_proto.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_proto.yml)
[![Rust CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_rust.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_rust.yml)
[![Rust CI (Nightly)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_with_rust_nightly.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_with_rust_nightly.yml)
[![Wren CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_wren.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_wren.yml)
[![Python Client CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_client_python.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_client_python.yml)
[![Notebooks CI](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_notebooks.yml/badge.svg)](https://github.com/kaskada-ai/kaskada/actions/workflows/ci_notebooks.yml)

Kaskada is a query engine for event-based (timestamped).

Kaskada unifies real-time and historical data processing allowing practitioners to deploy real-time queries with access to their full event history and to instantly reconstruct the results a query would have produced at arbitrary, data-dependent times in the past.

Kaskada’s core innovations are a set of time-aware query abstractions, a high-performance compute engine optimized for processing event data, and a flexible execution model supporting both bulk and real-time data processing.

These features were chosen to make Kaskada a perfect fit for Feature Engineering on event-based data, as part of Real-Time ML. With Kaskada you can seamlessly move from interactive feature exploration in a notebook to real-time feature serving in production without any changes to your feature definitions.

## Development Setup

### Mac OS
* Install [LLVM/Clang](https://clang.llvm.org/get_started.html) via the XCode tools.
* Install protoc `brew install protobuf`

### Linux (Debian-based)
* `apt install clang lld libssl-dev pkg-config protobuf-compiler`

### All platforms: install Rust
* Install Rust using [rustup](https://www.rust-lang.org/tools/install).
* Install the following Rustup components
    * `rustup component add rust-src` -- Rust source code for IDE completion.
    * `rustup component add clippy rustfmt` -- Rust linter and formatter.

Run `cargo test` to verify everything is working.

### Testing & Building the Compute Engine

Running `cargo test` will run all the tests for the compute engine.

Run `cargo build --release -p sparrow-main` to build a release (optimized) binary of the main executable.

### Configurations
* `TMPDIR` - The compute engine uses temporary files as part of computation. By default, this uses the default temporary file directory (platform dependent). See: [tempfile::NamedTempFile](https://docs.rs/tempfile/1.1.2/tempfile/struct.NamedTempFile.html). To set the temporary path directory, set the `TMPDIR` environment variable. See: [std::env::temp_dir](https://doc.rust-lang.org/std/env/fn.temp_dir.html).

### Running integration tests

#### Pre-requisites
* ensure docker is running locally
* run `make proto/generate` and `make ent/generate`.  See the `./wren/README.md` for more info on those.

#### To run the tests with local object storage and sqlite db
* run `make test/int/docker-up` in one terminal window to get the Kaskada service and dependencies up
* run `make test/int/run-api` in another terminal window to run the API integration tests

After making code changes, `ctrl-c` in the services window and restart it.

**Note:** that there are also other make commands to test the other supported object stores and databases. Append any of the following on your make commands to test other scenarios:
* `-s3`: s3 object storage (minio) with sqlite db
* `-postgres`: local object storage with postgres db
* `-postgres-s3`: s3 object storage (minio) with postgres db

### Visual Studio Code

* Install [Visual Studio Code (VSC)](https://code.visualstudio.com/download)
* Install the following VSC extensions
*  * [Rust Analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer). Essential.
    Provides the language server integration for Rust code.
*  * [Even Better TOML](https://marketplace.visualstudio.com/items?itemName=tamasfe.even-better-toml). Optional.
*  * [Cargo](https://marketplace.visualstudio.com/items?itemName=panicbit.cargo). Optional.
*  * [Crates](https://marketplace.visualstudio.com/items?itemName=serayuzgur.crates) Optional.

## Links

* [Getting Started](docs/getting_started.adoc) for more information.
