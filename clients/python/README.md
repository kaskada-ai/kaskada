# Kaskada SDK

## Developer Instructions
The package uses Poetry to develop and build.

1. Install Pyenv [Pyenv Documentation](https://github.com/pyenv/pyenv)
1. Install Python 3.9.16: `$ pyenv install 3.9.16`
1. Install Poetry [Poetry Documentation](https://python-poetry.org/docs/)
1. Install dependences: `$ poetry install`

### Run tasks
The package uses [`poethepoet`](https://github.com/nat-n/poethepoet) for running tasks

#### Test Task
To run tests: `$ poetry run poe test` 

#### Check Style Task
To check the style: `$ poetry run poe style`

#### Format Task
To auto-format (isort + black): `$ poetry run poe format`

#### Check Static Type Task
To perform static type analysis (mypy): `$ poetry run poe types`

#### Lint Task
To run the linter (pylint): `$ poetry run poe lint`

#### Generate documentation 
We use Sphinx and rely on autogeneration extensions within Sphinx to read docstrings and 
generate an HTML formatted version of the codes documentation. 

* `poetry run poe docs` : generates and builds the docs 
* `poetry run poe docs-generate` : generates the docs (.rst files)
* `poetry run poe docs-build` : builds the HTML rendered version of the generated docs (from .rst to .html)

The generated HTML is located inside `docs/build`. Load `docs/build/index.html` in your browser to see the HTML rendered output. 

## Using the Client from Jupyter

#### Install Jupyter
To install Jupyter: `$ pip install notebook`

#### Build the Client
To build the client: `$ poetry build`

#### Install the Client
To install the client: `$ pip install dist/*.whl`

#### Open a Jupyter Notebook
To open a notebook: `$ jupyter notebook`
