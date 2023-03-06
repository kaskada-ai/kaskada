# Kaskada Examples

This directory contains different examples of using Kaskada. These examples should work on the latest release of Kaskada and the latest Kaskada python client. The intent is to ensure these examples are kept up to date and are working reference points for customers.


## In Development Examples

Examples prefixed with **[Pending]** are pending release. The content in the examples are not officially available and may require building the binaries + clients locally to run. 

*Note: Using local builds requires informing the python client to use the local build (by default uses the latest release). This is achieved by passing the `download(False)` build param. See below:* 

```python 
from kaskada.api.session import LocalBuilder
session = LocalBuilder().download(False).build()
```

To build the binaries locally:
* `make build/manager` - Builds the manager/API binary.
* `make build/engine` - Builds the compute engine binary.
* `make build/services` - Builds all the services.

To build the python client locally:
* `make build/python` - Builds the python client locally.
* `make install/python` - Builds and installs the python client locally.
