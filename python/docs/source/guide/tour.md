---
file_format: mystnb
kernelspec:
  name: python3
  display_name: Python 3
mystnb:
  execution_mode: cache
---

% Level: Beginner
% Goal: Overview of the key features of Kaskada focused on explaining *why* you want them.
% Audience: Someone who has read the landing page and wants to understand what Kaskada can do for them.

# Tour of Kaskada

This provides an overview of the key features in Kaskada that enable feature engineering on event-based data.
The [Quick Start](./quickstart.md) has details on how you can quickly get started running Kaskada queries.
For a more complete explanation, see the User Guide.

This tour uses Kaskada and Plotly to render the illustrations.
The initial setup / data is below.

```{code-cell}
---
tags: [hide-cell]
---
import kaskada as kd
kd.init_session()
single_entity = "\n".join(
    [
        "time,key,m,n",
        "1996-12-19T16:39:57,A,5,10",
        "1996-12-20T16:39:59,A,17,6",
        "1996-12-22T16:40:00,A,,9",
        "1996-12-23T16:40:01,A,12,",
        "1996-12-24T16:40:02,A,,",
    ]
)
single_entity = kd.sources.CsvString(single_entity, time_column="time", key_column="key")
```

## Events and Aggregations

Every Kaskada query operates on one or more _sources_ containing events.
Every event in a source happens at a specific point in time and relates to a specific entity.
A source contains events with the same schema.
Often, each source represents a specific kind of event, such as a login event or purchase.

It is often convenient to picture temporal data as a sequence of timestamped events.
A natural question to ask about the purchases is the total--or `sum`--of all purchases made.
This is accomplished by _aggregating_ the events.
The results of an aggregation change over time as additional events occur.

```{code-cell}
---
tags: [remove-input]
---
kd.plot.render(
    kd.plot.Plot(single_entity.col("m"), name="m"),
    kd.plot.Plot(single_entity.col("m").sum(), name="sum of m")
)
```

The User Guide has [more details on aggregation](./aggregation.md), including how to use windows to control which events are aggregated.

## Discrete and Continuous
We say that events (and values derived from them) are _discrete_ because they occur at specific in time.
and the results of the aggregation are [_continuous_](./timestreams.md#continuity).
In the example, after the purchase with amount 13 the sum was 20.
And it _continued_ to be 20 at every point in time until the next purchase was made, with amount 4.
A continuous value is inclusive of the event that causes the value to change and exclusive of the next change.

Thus, an aggregation at a given point in time reflects all events that have happened up to (and including) that point in time.
The concept of continuity applies to many other operations in Kaskada, not just aggregations.
This is part of what we mean when we say that Kaskada is a temporal query language.

## Grouping
Another property of Kaskada is that events are implicitly grouped by _entity_.
In the previous example, we assumed that all purchases were made by the same user.
When the purchases are made by multiple users, there is a natural grouping for the aggregation.
When computing a machine learning feature such as "total purchases", we usually wish to aggregate the events related to a specific user or entity.

One way to understand this grouping is as a separate stream associated with each entity.
The stream of purchases for each user may be shown separately, as we do here, or it may be pictured flattened into a single stream keyed by user.
The idea of grouped streams as separate, per-entity streams is often useful for understanding the behavior of Kaskada Timestreams.

```{todo}
Add example of multiple entity aggregation.
```

The User Guide has [more details on grouping](./entities.md), including how to change the grouping of a Timestream.

## History and Snapshots

Since the Timestream describes how values are computed at every point in time, there are several useful ways they may be output.

For training a model, it is often useful to output historic values matching some `filter`.
These historic points can then be used as training examples, allowing the model to be trained on past points.
This historic output is also useful for visualizing a Timestream at multiple points.

For serving a model, it is often useful to output the value of a Timestream for every entity at a specific point in time.
This is most often used to output a snapshot at the current time.

For both kinds of output, it is also useful to be able to select only the points after a specific time.
This would filter out points from the history, or limit the snapshot to only those entities which have changed.

## Windowed Aggregation

```{todo}
Update to reflect actual syntax. Include example.
```

In addition to the default behavior of aggregating over all events up to a given time, aggregations may be performed over specific windows.
For example, `hourly()` describes periodic windows of an hour.
The aggregation, `sum(Purchases, window=hourly())` would produce the cumulative sum of purchases made since the beginning of the hour.
For example, if there were purchases at 8:45 AM, 9:15 AM and 9:25 AM and 10:02 AM, then the result at 9:25 AM is the sum from 9:00 AM to 9:25 AM, which would include only the events at 9:15 AM and 9:25 AM.

A non-cumulative windowed aggregation produces values only at the end of a window.
For instance, `sum(Purchases, window=hourly(), cumulative=false)` will produce the sum for the past hour.
With the purchases in the previous example, this would mean that at 9:00 AM an event is produced containing the amount of the purchase at 8:45 AM, and at 10:00 AM an event is produced containing the sum of the purchases at 9:15 AM and 9:25 AM.
A window must be specified when using a non-cumulative aggregation.

The section on [Aggregation](./aggregation.md#windowing) has more information on windowing.