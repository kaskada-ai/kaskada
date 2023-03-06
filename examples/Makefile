# Make file is intended for local development/running pending examples.
# The majority of examples are executable without building the binaries or clients locally.

# Build the manager service and store it for client default consumption
build/manager:
	cd ../; make ent/clean ent/generate proto/generate wren/build;
	mkdir -p ~/.cache/kaskada/bin
	cp ../wren/main ~/.cache/kaskada/bin/kaskada-manager

# Build the compute engine service and store it for client default consumption
build/engine:
	cd ../; cargo build --bin sparrow-main --release --all-features;
	mkdir -p ~/.cache/kaskada/bin
	cp ../target/release/sparrow-main ~/.cache/kaskada/bin/kaskada-engine

# Build all the services
build/services: build/manager build/engine

# Build the python client
build/python:
	cd ../clients/python; poetry build;

# Install the python client locally
install/python: build/python
	pip install --force-reinstall --upgrade ../clients/python/dist/*.whl
