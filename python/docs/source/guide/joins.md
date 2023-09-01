# Joins

It is often necessary to use multiple Timestreams to define a query.
Understanding user behavior requires considering their activity across a variety of event streams.
Normalizing user behavior may require looking comparing the per-user values to the average values for the region.
Both of these are accomplished by joining the Timestreams.

## Implicit Joins
Timestreams associated with the same kind of entity -- for instance, a user -- are automatically joined.
This makes it easy to write queries that consider multiple event streams in a single query.

```{code-block} python
:caption: Joining two event streams to compute page-views per purchase

page_views.count() / purchases.count()
```

### Domains

It is sometimes useful to consider the _domain_ of an expression.
This corresponds to the points in time and entities associated with the points in the expression.
For discrete timestreams, this corresponds to the points at which those values occur.
For continuous timestreams, this corresponds to the points at which the value changes.

Whenever expressions with two (or more) different domains are used in the same expression they are implicitly joined.
The join is an outer join that contains an event if either (any) of the input domains contained an event.
For any input table that is continuous, the join is `as of` the time of the output, taking the latest value from that input.

## Explicit Lookups

Values from associated with other entities may be retrieved using {py:meth}`kaskada.Timestream.lookup`.
`left.lookup(right)` does a left-join, looking up the value from `right` for each computed key in `left`.

Lookups are _temporally correct_ -- the value retrieved corresponds to the `right` value at the time the key occurred in `left`.