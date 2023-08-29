# Sources

Sources describe how events enter a Timestream.
Every source is a Timestream containing the events that have been read.
Most often, these events are [records](data_types.md#record-types).

Each event from a source is associated with a specific time and entity key.
These define how the resulting Timestream is ordered and grouped.
Generally, the time and entity key are associated with a specific column from the source, using the `time_column` and `key_column` arguments.

The [Sources Reference](../reference/sources.md) has more details on the supported sources.