# User Guide

Understanding and reacting to the world in real-time requires understanding what is happening _now_ in the context of what happened in the past.
You need the ability to understand if what just happened is unusual, how it relates to what happened previously, and how it relates to other things that are happening at the same time.

Kaskada processes events from streams and historic data sources to answer these questions in real-time.

The power and convenience of Kaskad comes from a new: the Timestream.
Timestreams provide a declarative API like dataframes over the complete temporal context.
Easily combine multiple streams and reason about the complete sequence of events.
Use time-travel to compute training examples from historic data and understand how results change over time.

## What are "Timestreams"?

A [Timestream](timestreams) describes how a value changes over time.
In the same way that SQL queries transform tables and graph queries transform nodes and edges, Kaskada queries transform Timestreams.

In comparison to a timeseries which often contains simple values (e.g., numeric observations) defined at fixed, periodic times (i.e., every minute), a Timestream contains any kind of data (records or collections as well as primitives) and may be defined at arbitrary times corresponding to when the events occur.

## Getting Started with Timestreams

Getting started with Timestreams is as simple as `pip` installing the Python library, loading some data and running a query.

```python
import timestreams as t

# Read data from a Parquet file.
data = t.sources.Parquet.from_file(
    "path_to_file.parquet",
    time = "time",
    key = "user")
# Get the count of events associated with each user over time, as a dataframe.
data.count().to_pandas()
```

```{toctree}
:hidden:
:maxdepth: 2

quickstart
tour
why
installation
timestreams
data_types
entities
aggregation
joins
sources
execution
```