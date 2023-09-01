# Aggregation

Aggregations are _cumulative_, _grouped_, and _windowed_.

Cumulative
: They reflect all values up to and including the current time.

Grouped
: They reflect the values for each entity separately.

Windowed
: They reflect the values within a specific [window](../reference/windows.md).

While most functions operate on specific points in the Timestream, aggregation functions operate on many points.
Generally, the result of an aggregation represents the result for each key up to and including the current time.

## Windowing
Aggregations may be configured to operate in a specific time window by providing a `window` argument.
If no window is specified, the aggregation is over all rows for the entity, up to and including the current time.
If a window is provided, the result of an aggregation is the result for that entity in the current window up to and including the current time.
Aggregations produce cumulative results up to each point in time, so the result at a given point in time may represent an incomplete window.

```{code-block} python
:caption: Cumulative aggregation since the start of the day.
Purchases.sum(window = kd.windows.Since.daily())
```

The [windows reference](../reference/windows.md) has information on the supported kinds of windows.

## Repeated Aggregation

Events may be aggregated multiple times.
The events themselves are a sequence of timestamped data for each entity.
The result of the first aggregation is the same — a sequence of timestamped data for each entity.
Applying an additional aggregation simply aggregates over those times.
We can compute the maximum of the average purchase amounts.

```{code-block} python
:caption: Repeated aggregation computing the maximum of the average purchases.
Purchases.col("amount").mean().max()
```