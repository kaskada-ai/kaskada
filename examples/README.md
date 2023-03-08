# Kaskada Examples

This directory contains different examples of using Kaskada. The included examples should work with the latest [release of Kaskada](https://github.com/kaskada-ai/kaskada/releases) and the latest Python Client [released to PyPi](https://pypi.org/project/kaskada/#history). However, there are some examples that may be pending/in-development (see section below). The intent is to ensure these examples are kept up to date and are working reference points for users.


## In Development Examples

There may be some examples that are demonstrate functionality not officially available yet. To view the latest functionality, it may require building the binaries + clients locally to run. 

*Note: Using local builds requires informing the python client to use the local build (by default uses the latest release). This is achieved by passing the `download(False)` build param. See below:* 

```python 
from kaskada.api.session import LocalBuilder
# download(False) skips the fetch of the latest 
session = LocalBuilder().download(False).build()
```

**To run everything (build services + build client + run jupyter): `make start/jupyter`**

To build the binaries locally:
* `make build/services` - Builds all the services.
    * `make build/manager` - Builds the manager/API binary.
    * `make build/engine` - Builds the compute engine binary.

To build the python client locally:
* `make venv/build-install-clients` - Build and install the clients
    * `make venv/create` - Create virtual environment and install dependencies
    * `make venv/build-client-python` - Builds the python client locally.
    * `make venv/install-client-python` - Builds and installs the python client locally.

To clean:
* `make clean` - Cleans everything (notebooks, build, and virtual environment)
    * `make build/clean` - Cleans the built binaries locally and from cache directory
    * `make venv/notebook/clean` - Cleans all output from notebook (use prior to committing)
    * `make venv/clean` - Nukes the virtual environment