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

```{todo}
Example of grouped streams and aggregation
```

## Joining

Joining with the same entity happens automatically.
Joining with other entities (and even other kinds of entities) is done using `lookup`.
See [Joins](joins.md) for more information.