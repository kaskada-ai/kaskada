# Aggregation

Timestream aggregations are:

Cumulative:
    They reflect all values up to and including the current time.
Grouped:
    They reflect the values for each entity separately.
Windowed:
    They reflect the values within a specific [window](../windows.md).

```{eval-rst}
.. currentmodule:: kaskada

.. autosummary::
   :toctree: ../apidocs/

    Timestream.collect
    Timestream.first
    Timestream.last
    Timestream.sum
```