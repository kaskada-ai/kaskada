## _scripts

The files in this folder are used by quartodoc to
generate custom output for the Python API docs.

#### custom renderer (and summarizer)

Start here if you want to change any of the markdown generated for our API docs.

`renderer.py` and `summarizer.py` are based off of the [default quartodoc renderer](https://github.com/machow/quartodoc/blob/main/quartodoc/renderers/md_renderer.py),
split into 2 files.

Both of these files use the `@dispatch` decorator to overload the same method
name on many different input types. It can be tricky to find the correct
method to update.

#### custom builder

Start here if you want to change how & where we output individual markdown files
for our API docs.

* `builder.py` iterates over a passed config file and then uses `renderer.py` and `summarizer.py`
to output markdown for the API docs. It based off the [default quartodoc builder](https://github.com/machow/quartodoc/blob/v0.6.3/quartodoc/autosummary.py#L367),
but is heavily modified.
* `gen_reference.py` sets up logging, and calls `builder.py` with the `../_reference.yml` config file

Note that we output files in a VERY different way than the default quartodoc executable behaves.
For instance, we rely on the path of each file to build the side-bar navigation instead of
generating a specific side-bar layout.

#### linter

This set of scripts ensures that we have included all python methods and classes in our docs.

* `linter.py` is based off of `builder.py`
* `lint_reference.py` sets up logging, and calls `linter.py` with the `../_reference.yml` config file
