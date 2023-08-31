---
file_format: mystnb
kernelspec:
  name: python3
  disply_name: Python 3
mystnb:
  execution_mode: cache
---

# Entities and Grouping

Entities organize data for use in feature engineering.
They describe the particular objects that a prediction will be made for.
The result of a feature computation is a _feature vector_ for each entity at various points in time.

## What is an Entity?
Entities represent the categories or "nouns" associated with the data.
They can generally be thought of as any category of object related to the events being processed.
For example, when manipulating purchase events, there may be entities for the customers, vendors and items being purchased.
Each purchase event may be related to a customer, a vendor, and one or more items.

If something can be given a name or other unique identifier, it can likely be used as an entity.
In a relational database, an entity would be anything that is identified by the same key in a set of tables.

## What is an Entity Key?
An entity kind is a category of objects, for example customer or vendor.
An entity key identifies a unique instance of that category -- a `customer_id` or a `vendor_id`.

One may think of an entity as a table containing instances -- or rows -- of that type of entity.
The entity key would be the primary key of that table.

The following table shows some example entities and possible keys.
Many of the example instances may not be suitable for use as the entity key, for the same reason you wouldn't use them as a primary key.
For example, using `Vancouver` to identify cities would lead to ambiguity between Vancouver in British Columbia and Vancouver in Washington State.
In these cases, you'd likely use some other identifier for instances.
Others may be useful, such as using the airport code.

:::{list-table} Example Entities and corresponding keys.
:header-rows: 1

* - Example Entity
  - Example Entity Instance
* - Houses
  - 1600 Pennsylvania Avenue
* - Airports
  - SEA
* - Customers
  - John Doe
* - City
  - Vancouver
* - State
  - Washington
:::

## Entities and Aggregation

Many, if not all, Kaskada queries involve aggregating events to produce values.
Entities provide an implicit grouping for the aggregation.
When we write `sum(Purchases.amount)` it is an aggregation that returns the sum of purchases made _by each entity_.
This is helpful since the _feature vector_ for an entity will depend only on events related to that entity.

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
    kd.plot.Plot(multi_entity.col("m"), name="m"),
    kd.plot.Plot(multi_entity.col("m").sum(), name="sum(m)")
)
```


## Changing Keys

The key associated with each point may be changed using {py:meth}`kaskada.Timestream.with_key`.
For example, given a stream of purchases associated with each user, we could create a Timestream associated with the purchased item:

```{code-block} python
:caption: Using with-key to associate purchases with items
purchases_by_user.with_key(purchases_by_user.col("item_id"))
```

This is particularly useful with the ability to [lookup](joins.md#explicit-lookups) values from associated with other keys.
For instance, we could re-key purchases as shown above to count the total spent on each item (across all users) and then lookup that information for each user's purchases.

## Joining

Joining with the same entity happens automatically.
Joining with other entities (and even other kinds of entities) is done using `lookup`.
See [Joins](joins.md) for more information.