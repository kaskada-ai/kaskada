# Kaskada Timestreams

## Developer Instructions
The package uses Poetry to develop and build.

1. Install Pyenv [Pyenv Documentation](https://github.com/pyenv/pyenv)
1. Install Python 3.9.16: `$ pyenv install 3.9.16`
1. Install Poetry [Poetry Documentation](https://python-poetry.org/docs/)
1. Install dependences: `$ poetry install`

#### Build the Package
To build the client: `$ poetry build`

#### Publishing the Package to PyPi
* build the package (see above)
* set the `POETRY_PYPI_TOKEN_PYPI` env var in your environment
* from the `./clients` folder:, run `$ docker compose run push-timestreams`
