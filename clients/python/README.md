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
