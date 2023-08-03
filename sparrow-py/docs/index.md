---
hide-toc: true
---

# Kaskada Timestreams

```{include} ../README.md
:start-after: <!-- start elevator-pitch -->
:end-before: <!-- end elevator-pitch -->
```

## Getting Started with Timestreams

Getting started with Timestreams is as simple as `pip` installing the Python library and

```python
import timestreams as t

# Read data from a Parquet file.
data = t.sources.Parquet.from_file(
    "path_to_file.parquet",
    time = "time",
    key = "user")
# Get the count of events associated with each user over time, as a dataframe.
data.count().run().to_pandas()
```

## What are "Timestreams"?
A [Timestream](reference/timestream) describes how a value changes over time. In the same way that SQL
queries transform tables and graph queries transform nodes and edges,
Kaskada queries transform Timestreams.

In comparison to a timeseries which often contains simple values (e.g., numeric
observations) defined at fixed, periodic times (i.e., every minute), a Timestream
contains any kind of data (records or collections as well as primitives) and may
be defined at arbitrary times corresponding to when the events occur.

```{toctree}
:hidden:

quickstart
```

```{toctree}
:caption: Reference
:hidden:

reference/timestream
reference/sources
reference/execution
```