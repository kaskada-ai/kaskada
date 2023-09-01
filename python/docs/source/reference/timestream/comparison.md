# Comparison

Comparison operations produce boolean Timestreams.

```{note}
Note: In addition to the chainable methods, standard operators are implemented where appropriate.
For instance, `a.ge(b)` may be written as `a >= b`.
See the notes on the specific functions for more information.

To respect the semantics of `__eq__` and `__ne__`, `a == b` and `a != b` are *not* overloaded.
```

## Comparison Methods

```{eval-rst}
.. currentmodule:: kaskada

.. autosummary::
   :toctree: ../apidocs/

    Timestream.eq
    Timestream.ge
    Timestream.gt
    Timestream.le
    Timestream.lt
    Timestream.ne
    Timestream.is_null
    Timestream.is_not_null
```