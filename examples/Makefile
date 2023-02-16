build/clean:
	rm -f sparrow-main wren-main
	rm -f ~/.kaskada/bin/*
	rm -f ../wren/wren-main
	rm -f ../target/release/sparrow-main

build/wren:
	cd ../; cp NOTICE wren/; make proto/generate; make ent/generate;
	go build -o ~/.cache/kaskada/bin/kaskada-manager  ../wren/main.go

build/crates:
	cargo build --bin sparrow-main --release -p sparrow-main
	cp ../target/release/sparrow-main ~/.cache/kaskada/bin/kaskada-engine

build/services: build/clean build/wren build/crates

venv/create:
	python3 -m venv env; source env/bin/activate; pip install -r requirements.txt;
	source env/bin/activate; cd ../clients/python; poetry install;

venv/jupyter:
	source env/bin/activate; jupyter notebook --port 1225;

venv/notebook/clean:
	source env/bin/activate; jupyter nbconvert --clear-output --inplace *.ipynb

jupyter/stop:
	lsof -t -i tcp:1225 | xargs kill

venv/clean: jupyter/stop
	rm -rf env;
	rm -rf ../clients/python/dist;

venv/update-dependencies:
	source env/bin/activate; pip freeze > requirements.txt;

venv/build-client-python:
	cd ../proto; buf generate;
	source env/bin/activate; cd ../clients/python; rm -rf dist; poetry build;

venv/install-client-python:
	source env/bin/activate; pip install ../clients/python/dist/*.whl;

venv/build-install-clients: venv/build-client-python venv/install-client-python

venv/reset: clean venv/create venv/build-install-clients

venv/uninstall-clients: 
	source env/bin/activate; pip uninstall -y kaskada;

venv/re-jupyter: jupyter/stop venv/uninstall-clients venv/build-install-clients venv/jupyter

start/jupyter: venv/reset build/services venv/jupyter

clean: build/clean venv/clean