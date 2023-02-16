# Examples

## Requirements
* Python 3.10+
* Poetry
* Go
* Cargo/Rust

## Getting Started
The local example runs are simplified using the local `Makefile`. Most commands revolve around a **virtualenv**.
To create a virtual environment: `make venv/create`. All make commands prefixed with `venv` will activate the virtual environment prior to starting.

### Clean
If you are experiencing some weirdness, a clean command exists to delete the virtual environment and all temporary builds: `make venv/clean`.

### Start Jupyter
A local version of Jupyter wrapped in the virtualenv can be launched using: `make venv/jupyter`.

### Stop Jupyter 
To stop Jupyter: `make jupyter/stop` (uses lsof to kill Jupyter on the port).

### Notebook Cleaning
There is a Github Action to check if metadata from the notebooks are stripped. To remove metadata: `make venv/notebook/clean`.

### Services
* `make build/wren` - Generates the protos, ends and the API binary.
    * `make start/wren` - Runs the API binary with some basic environment variables.
* `make build/crates` - Builds the compute engine binary.
    * `make start/crates` - Runs the compute binary with some basic environment variables.
* `make build/services` - Performs a clean, then builds the API and compute engine binary.
* `make start/minio` - Runs minio configured locally (TODO: this will be removed once local files is complete.)

### Updating the Example Dependencies
Occassionally, additional dependencies may be needed to run locally on virtual environments. These dependencies are stored in `requirements.txt`. To update which ones are required/installed: `make venv/update-dependencies`.

### Building the clients:
The examples use the Python and Fenlmagic clients. These libraries should be available publicly on PyPi but to use the development versions:
* Python Client
    * Build - `make venv/build-client-python`. Generates the Python protobufs, cleans, and builds wheel using Poetry.
    * Install - `make venv/install-client-python`. Installs the wheel generated from the build in the `clients/python/dist` directory.
* Fenlmagic Client
    * Build - `make venv/build-client-fenlmagic`. Cleans, and builds wheel using Poetry.
    * Install - `make venv/install-client-fenlmagic`. Installs the wheel generated from the build in the `clients/fenlmagic/dist` directory.
* Build and install both: `make venv/build-install-clients`.

### Reset and install clients
A shortcut for cleaning everything, recreating a new virtual envirnment and build-install clients: `make venv/reset`.

### Re-install local clients
Stops Jupyter, uninstalls the clients, builds/reinstalls clients, and launches Jupyter: `make venv/re-jupyter`.
