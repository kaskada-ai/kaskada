# Why Kaskada?

Kaskada is a library for executing temporal queries over event-based data.
An "event" can be any fact about the world associated with a time.
For example, a user signing up for a service, or a customer purchasing a product.
As additional events occur the computed values may change as well.

Traditional data processing systems are designed to answer questions about the current state of a dataset.
For instance, "how many purchases has a given user made?"
Over time, the user makes additional purchases and the answer *should* change.
With these traditional data processing systems, the answer changes based on when it is asked.

With Kaskada, the query "how many purchases has a given user made?" is expressed as a _Timestream_.
This represents how the result of that query changes over time for each user.
Kaskada makes it easy to combine Timestreams to produce a new Timestream -- joining points from each input as needed.