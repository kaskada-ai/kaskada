# Execution

A [Timestream](./timestreams.md) may be executed and written to a [destination](#destinations) in a variety of ways.
The things to consider when deciding how to execute the Timestream are:

1. Whether you want the _history_ of points or the _snapshot_ of values for each entity at a given time.
2. Whether you want to run a query _once_ or start a _live_ process continually materializing.
3. Whether you want to limit the output to points in a specific time range (for history) or entities that have changed since a specific time (for snapshots).
4. Whether you want to stop at a given point in time.

[State](#state) can be used to provide fault-tolerance and allow incremental processing of only new events.

```{admonition} Preview during development
:class: tip

While developing queries, it is often useful to view a few rows from the result.
Using {py:meth}`kaskada.Timestream.preview` you can retrieve a small set of rows from the result set as a Pandas DataFrame.
```

## History vs. Snapshot

Executing a Timestream for the history outputs every point in the Timestream.
This means that each entity likely appears in the output multiple times.
This is particularly useful for creating training examples from past data points, or visualizing how the Timestream has changed over time.

Executing a Timestream for a snapshot produces a value for each entity at a specific point in time.
This means that each entity appears at-most once in the results.
This is useful for maintaining a feature store based on the latest values.

```{todo}
Expose the configuration for snapshots.
See https://github.com/kaskada-ai/kaskada/issues/719
```

## Query vs. Materialize
Every Timestream may be executed as a single query or used to start a materialization.
Single queries are useful when you want the results for some later batch process, such as fine-tuning a model or populating an in-memory feature store.
Materialization is useful when you want to stream new results out as quickly as possible, such as maintaining an in-memory feature store or reacting to specific conditions.


## Changed Since

Configuring the _changed since time_ lets you control the points or entities included in the output.

For a historic query, only points occurring after the changed since time are included in the output.
This allows incrementally outputting the entire history to some external store, by repeatedly performing a "changed since" query.

For a snapshot query, only entities that have changed after this time are included in the output.
This reduces the amount of data written when the past snapshot is already present in the destination.

```{todo}
Expose the configuration for changed since.
See https://github.com/kaskada-ai/kaskada/issues/719
```

## Up To

Configuring the _up to time_ lets you control the maximum points output (and in the case of snapshots, the time represented in the snapshot).

For a historic query, only points occurring before or at the up to time are included in the output.
For a snapshot query, this corresponds to the time at which the snapshot will be taken.

```{note}
Currently when not specified, the up to time is determined from the maximum event present in the data.
We have plans to change this to a parameter to `run` defaulting to the current time.
```

```{todo}
Expose the configuration for up-to.
See https://github.com/kaskada-ai/kaskada/issues/719
```

## State

Kaskada checkpoints state during and after execution.
This provides fault-tolerance, incremental querying and automatic handling of late-data.

When a query is executed, Kaskada determines whether it can use any of the available states to reduce the amount of processing needed.
For instance, when producing a snapshot Kaskada can use any persisted state before the earliest new event and before the time to snapshot.
Similarly, when producing a history, Kaskada can use any persisted state before the earliest new event and before the "changed since" time.

## Destinations

The methods {py:func}`Timestream.preview<kaskada.Timestream.preview>` and {py:func}`Timestream.to_pandas<kaskada.Timestream.to_pandas>` provide the results of a query in a Pandas DataFrame for easy visualization and consumption within the Python process.

The {py:func}`Timestream.run_iter<kaskada.Timestream.run_iter>` methods provides synchronous and asynchronous iterators over the results in a variety of formats including Pandas DataFrames, PyArrow RecordBatches, and rows as Python dictionaries.
This allows you to run the entire retrieve-evaluate-respond loop within a single Python process.

The {py:func}`Timestream.write<kaskada.Timestream.write>` function allows you to specify a destination from {py:mod}`kaskada.destinations` for results.
This supports both `once` and `live` queries
See the reference on [destinations](../reference/destinations.md) for more on the supported destinations.