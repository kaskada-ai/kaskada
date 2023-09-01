---
file_format: mystnb
kernelspec:
  name: python3
  disply_name: Python 3
mystnb:
  execution_mode: cache
---

# Timestreams

Kaskada is built on the concept of a _Timestream_.
Each Timestream is ordered by _time_ and partitioned by _entity_.
This makes it easy to focus on events happening over time and how aggregations change.

% The input for this needs to be hidden, not removed. It seems that plotly won't
% render the right height otherwise (possibly because it's not attached to the DOM?).
% We could see if this worked better using a different library such as `bokeh` or if
% there were better options to pass to plotly to avoid the problem.
```{code-cell}
---
tags: [hide-input]
---
import kaskada as kd
kd.init_session()
data = "\n".join(
    [
        "time,key,m",
        "1996-12-19T16:39:57,A,5",
        "1996-12-19T17:42:57,B,8",
        "1996-12-20T16:39:59,A,17",
        "1996-12-23T12:18:57,B,6",
        "1996-12-23T16:40:01,A,12",
    ]
)
multi_entity = kd.sources.CsvString(data, time_column="time", key_column="key")

kd.plot.render(
    kd.plot.Plot(multi_entity.col("m"), name="m")
)
```

## Continuity

It is useful to consider two kinds of timestreams -- _discrete_ and _continuous_.
Like the example we already saw, a discrete timestream consists of values at specific points in time.
On the other hand, a continuous timestream represents a value that continues until it changes.
While a discrete timestream contains values at specific points in time, a continuous timestream changes at specific points in time.

For example, the result of aggregating a timestream produces a continuous stream that changes on each non-`null` input.

```{code-cell}
---
tags: [remove-input]
---
kd.plot.render(
    kd.plot.Plot(multi_entity.col("m").sum(), name="sum(m)")
)