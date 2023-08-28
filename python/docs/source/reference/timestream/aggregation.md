# Aggregation

Timestream aggregations are:

Cumulative
: They reflect all values up to and including the current time.

Grouped
: They reflect the values for each entity separately.

Windowed
: They reflect the values within a specific [window](../windows.md).

## Aggregation Methods

```{eval-rst}
.. currentmodule:: kaskada

.. autosummary::
   :toctree: ../apidocs/

    Timestream.collect
    Timestream.count
    Timestream.count_if
    Timestream.first
    Timestream.last
    Timestream.max
    Timestream.mean
    Timestream.min
    Timestream.stddev
    Timestream.sum
    Timestream.variance
```