# Joins


## Domains and Implicit Joins

It is sometimes useful to consider the _domain_ of an expression.
This corresponds to the points in time and entities associated with the points in the expression.
For discrete timestreams, this corresponds to the points at which those values occur.
For continuous timestreams, this corresponds to the points at which the value changes.

Whenever expressions with two (or more) different domains are used in the same expression they are implicitly joined.
The join is an outer join that contains an event if either (any) of the input domains contained an event.
For any input table that is continuous, the join is `as of` the time of the output, taking the latest value from that input.


## Implicit Joins

## Explicit Lookups