# Make file is intended for local development/running pending examples.
# The majority of examples are executable without building the binaries or clients locally.

build/clean:
	rm -f ~/cache/kaskada/bin/*
	rm -f ../wren/main
	rm -f ../target/release/sparrow-main

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

# Create the virtual environment and install dependencies
venv/create:
	python3 -m venv env; source env/bin/activate; pip install -r requirements.txt;
	source env/bin/activate; cd ../clients/python; poetry install;

# Start up Jupyter
venv/jupyter:
	source env/bin/activate; jupyter notebook --port 1225;

# Clean up notebooks by removing output and run numbers
venv/notebook/clean:
	source env/bin/activate; jupyter nbconvert --clear-output --inplace *.ipynb

# Forcibly stop Jupyter by killing the process on the port
jupyter/stop:
	lsof -t -i tcp:1225 | xargs kill

# Stop Jupyter and remove the virtual environment
venv/clean: jupyter/stop
	rm -rf env;

# Export the requirements.txt file
venv/export-requirements:
	source env/bin/activate; pip freeze --exclude kaskada > requirements.txt;

# Generate protos and build the python client whl
venv/build-client-python:
	cd ../proto; buf generate;
	source env/bin/activate; cd ../clients/python; rm -rf dist; poetry build;

# Install the latest built whl
venv/install-client-python:
	source env/bin/activate; pip install ../clients/python/dist/*.whl;

# Build and install the latest whl
venv/build-install-clients: venv/build-client-python venv/install-client-python

# Reset the virtual environment (clean, new environment, and build install)
venv/reset: clean venv/create venv/build-install-clients

# Uninstall the virtual environment Kaskada dependency
venv/uninstall-clients: 
	source env/bin/activate; pip uninstall -y kaskada;

# Reset the virtual environment with jupyter
venv/re-jupyter: jupyter/stop venv/uninstall-clients venv/build-install-clients venv/jupyter

# Start everything with jupyter
start/jupyter: venv/reset build/services venv/jupyter

# Clean everything
clean: jupyter/stop venv/notebook/clean build/clean venv/clean