# Contributing

When contributing to this repository, please first discuss the change you wish to make via a GitHub issue,
 or any other method with the owners of this repository before making a change.


## <a name="issue"></a> Found an Issue?

If you find any bugs in the source code and/or a mistake in the documentation, you can help us by
[submitting an issue](#submit-issue) to the GitHub Repository. Even better, you can
[submit a Pull Request](#submit-pr) with a fix.

## <a name="feature"></a> Want a Feature?

You can *request* a new feature by [submitting an issue](#submit-issue) to the GitHub
Repository. If you would like to *implement* a new feature, please submit an issue with
a proposal for your work first, to be sure that we can use it.

* **Small Features** can be crafted and directly [submitted as a Pull Request](#submit-pr).

## <a name="submit"></a> Contribution Guidelines

### <a name="submit-issue"></a> Submitting an Issue

Before you submit an issue, search the archive, maybe your question was already answered.

If your issue appears to be a bug, and hasn't been reported, open a new issue.
Help us to maximize the effort we can spend fixing issues and adding new
features, by not reporting duplicate issues.  Providing the following information will increase the
chances of your issue being dealt with quickly:

* **Overview of the Issue** - if an error is being thrown a non-minified stack trace helps
* **Motivation for or Use Case** - explain what are you trying to do and why the current behavior is a bug for you
* **Reproduce the Error** - provide a live example or a unambiguous set of steps
* **Suggest a Fix** - if you can't fix the bug yourself, perhaps you can point to what might be
  causing the problem (line of code or commit)

### <a name="submit-pr"></a> Submitting a Pull Request (PR)

Before you submit your Pull Request (PR) consider the following guidelines:

1. Search the repository (https://github.com/kaskada-ai/kaskada/pulls) for an open or closed PR that relates to your submission. You don't want to duplicate effort.

1. Create a fork of the repo
	* Navigate to the repo you want to fork
	* In the top right corner of the page click **Fork**:
	![](https://help.github.com/assets/images/help/repository/fork_button.jpg)

1. Create a new branch in your forked repository to capture your work. For example: `git checkout -b your-branch-name`

1. Commit changes to the branch  using a descriptive commit message
1. Make sure to test your changes using the unit and integration tests
1. When you’re ready to submit your changes, push them to your fork. For example: `git push origin your-branch-name`
1. In GitHub, create a pull request: https://help.github.com/en/articles/creating-a-pull-request-from-a-fork
1. If we suggest changes then:
  1. Make the required updates.
  1. Rebase your fork and force push to your GitHub repository (this will update your Pull Request):

    git rebase main -i
    git push -f

That's it! Thank you for your contribution!

## <a name="development"</a> Development

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

Note: All of the commands below should be run from the root of the repository.

#### in docker, with the local backend

* run `make test/int/docker-up` in one terminal window to get the Kaskada service and dependencies up
* run `make test/int/run-api-docker` in another terminal window to run the integration tests

After making code changes, `ctrl-c` in the services window and restart it.

#### in docker, with the s3 backend

* run `make test/int/docker-up-s3` in one terminal window to get the Kaskada service and dependencies up
* run `make test/int/run-api-s3-docker` in another terminal window to run the integration tests

After making code changes, `ctrl-c` in the services window and restart it.

#### locally, with the local backend

* run `make sparrow/run` in one terminal window to get the Engine service up
* run `make wren/run` in a second terminal window to get the Manager service up
* run `make test/int/run-api` in a third another terminal window to run the integration tests

After making code changes, `ctrl-c` in the proper service window and restart it.

#### locally, with the s3 backend

* run `make test/int/docker-up-s3-only` in one terminal window to get the dependencies up
* run `make sparrow/run-s3` in a second terminal window to get the Engine service up
* run `make wren/run-s3` in a third terminal window to get the Manager service up
* run `make test/int/run-api-s3` in a fourth terminal window to run the integration tests

After making code changes, `ctrl-c` in the proper service window and restart it.

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