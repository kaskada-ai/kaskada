% Level: Beginner
% Goal: Overview of the key features of Kaskada focused on explaining *why* you want them.
% Audience: Someone who has read the landing page and wants to understand what Kaskada can do for them.

# Tour of Fenl

This provides an overview of the key features in Kaskada that enable feature engineering on event-based data.
The [Quick Start](quickstart) has details on how you can quickly get started running Kaskada queries.
For a more complete explanation, see the User Guide.

## Events and Aggregations

Every Kaskada query operates on one or more _sources_ containing events.
Every event in a source happens at a specific point in time and relates to a specific entity.
A source contains events with the same schema.
Often, each source represents a specific kind of event, such as a login event or purchase.

It is often convenient to picture temporal data as a sequence of timestamped events.
A natural question to ask about the purchases is the total--or `sum`--of all purchases made.
This is accomplished by _aggregating_ the events.
The results of an aggregation change over time as additional events occur.

TODO: Port an example showing timestreams and aggregations.

[The guide][guide/aggregation.md] has more details on aggregations, including how to use windows to control which events are aggregated.

== Discrete and Continuous
We say that events (and values derived from them) are _discrete_ because they occur at specific in time.
and the results of the aggregation are [_continuous_](guide/timestreams.md#continuity).
In the example, after the purchase with amount 13 the sum was 20.
And it _continued_ to be 20 at every point in time until the next purchase was made, with amount 4.
A continuous value is inclusive of the event that causes the value to change and exclusive of the next change.

Thus, an aggregation at a given point in time reflects all events that have happened up to (and including) that point in time.
The concept of continuity applies to many other operations in Kaskada, not just aggregations.
This is part of what we mean when we say that Kaskada is a temporal query language.

== Grouping
Another property of Kaskada is that events are implicitly grouped by _entity_.
In the previous example, we assumed that all purchases were made by the same user.
When the purchases are made by multiple users, there is a natural grouping for the aggregation.
When computing a machine learning feature such as "total purchases", we usually wish to aggregate the events related to a specific user or entity.

One way to understand this grouping is as a separate stream associated with each entity.
The stream of purchases for each user may be shown separately, as we do here, or it may be pictured flattened into a single stream keyed by user.
The idea of grouped streams as separate, per-entity streams is often useful for understanding the behavior of Kaskada Timestreams.

TODO: Add example of multiple entity aggregation.

## Streams as Tables

In addition to visualizing the result of expressions as a stream, we can show them as a table.
When shown as a table, there is a column for each expression (stream) and a row for each point in the domain.

.Streams as Tables
====
The following table matches the results of the previous example.
Unlike the stream view which makes it easy to see the different events and which entity performed them, the table presents a flat view.
On the other hand, it is easy to see the domain since that is just the rows along with the time and entity in the table.
The table view is also useful for showing more information in less space.
It makes it easier to see both the amounts of purchases as well as the sum as it changes.

[cols="1,1,1,1"]
|===
|Time |Entity |`Purchase.amount` |`sum(Purchase.amount)`

| 2020-02-01 | Jordan | 2  | 2
| 2020-02-01 | Ryan   | 4  | 4
| 2020-02-02 | Ben    | 5  | 5
| 2020-02-03 | Ben    | 2  | 7
| 2020-02-03 | Jordan | 3  | 5
| 2020-02-04 | Jordan | 1  | 6
| 2020-02-05 | Jordan | 4  | 10
| 2020-02-06 | Ryan   | 9  | 13
| 2020-02-07 | Ben    | 13 | 20
| 2020-02-08 | Ben    | 4  | 24
|===
====

== Windowed Aggregation

In addition to the default behavior of aggregating over all events up to a given time, aggregations may be performed over specific windows.
For example, `hourly()` describes periodic windows of an hour.
The aggregation, `sum(Purchases, window=hourly())` would produce the cumulative sum of purchases made since the beginning of the hour.
For example, if there were purchases at 8:45 AM, 9:15 AM and 9:25 AM and 10:02 AM, then the result at 9:25 AM is the sum from 9:00 AM to 9:25 AM, which would include only the events at 9:15 AM and 9:25 AM.

A non-cumulative windowed aggregation produces values only at the end of a window.
For instance, `sum(Purchases, window=hourly(), cumulative=false)` will produce the sum for the past hour.
With the purchases in the previous example, this would mean that at 9:00 AM an event is produced containing the amount of the purchase at 8:45 AM, and at 10:00 AM an event is produced containing the sum of the purchases at 9:15 AM and 9:25 AM.
A window must be specified when using a non-comulative aggregation.

.Windowed Aggregation
====
The following example shows how to use windowed aggregations to compute the daily total of purchases.
Note that each window corresponds to an interval of time.
As shown, the interval is inclusive of the start of the window and exclusive at the end.

[stream_viz,name=windowed_aggregation,alt="Windowed aggregation"]
....
[
   {
    "label": "Purchase.amount",
    "kind": "discrete",
    "data": [
      [
        {"t": 2, "v": 5},
        {"t": 3, "v": 2},
        {"t": 7, "v": 13},
        {"t": 8, "v": 4}
      ]
    ]
  },
  {
    "label": "daily()",
    "kind": "windows",
    "data": [
      [0, 3],
      [3, 6],
      [6, 9]
    ]
  },
  {
    "label": "sum(Purchase.amount, window=daily())",
    "kind": "continuous",
    "data": [
      [
        {"t": 2, "v": 5},
        {"t": 3, "v": 2},
        {"t": 6, "v": null},
        {"t": 7, "v": 13},
        {"t": 8, "v": 17}
      ]
    ]
  }
]
....
====

As shown in the example, windowed aggregations produce the cumulative value in the current window as each event occurs.
See xref:concepts/aggregation.adoc[] for more details, including how to produce just the final value at the end of each window instead.

== Domains and Implicit Joins

It is sometimes useful to consider the _domain_ of an expression.
This corresponds to the points in time and entities associated with the points in the expression.
For discrete values, this corresponds to the points at which those values occur.
For continuous values, this corresponds to the points at which the value changes.

Whenever expressions with two (or more) different domains are used in the same expression they are implicitly joined.
The join is an outer join that contains an event if either (any) of the input domains contained an event.
For any input table that is continuous, the join is `as of` the time of the output, taking the latest value from that input.

The xref:spec/domains.adoc#multiple_domains_implicit_joins[Domains] section of the guide includes more details on implicit joins.