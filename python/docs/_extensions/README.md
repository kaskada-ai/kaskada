## _extensions

This is where quarto extensions are vendored.

#### why vendor?

I (Eric) thought it would be best to not have a copy of these here,
and instead install these fresh at CI run-time, but quarto
recommends doing it this way.

See: https://quarto.org/docs/extensions/managing.html#version-control

#### updating

There shouldn't be a need to update the extensions unless there is an
issue or a new desired feature. To update the extensions follow the
help here: https://quarto.org/docs/extensions/managing.html#updating
