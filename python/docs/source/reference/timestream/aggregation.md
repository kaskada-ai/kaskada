# Aggregation

The User Guide has details on [aggregations in general](../../guide/aggregation.md).

```{note}
It is important to remember that aggregations are partitioned by entity and windowed, with the default behavior being cumulative up to the current time.
```

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