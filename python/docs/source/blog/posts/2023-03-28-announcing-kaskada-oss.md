---
blogpost: true
author: ben
date: 2023-Mar-28
tags: releases
excerpt: 1
description: From Startup to Open Source Project
---

# Announcing Kaskada OSS

Today, we’re announcing the open-source release of Kaskada – a modern, open-source event processing engine.

# How it began: Simplifying ML

Kaskada technology has evolved a lot since we began developing it three years ago. Initially, we were laser focused on the machine learning (ML) space. We saw many companies working on different approaches to the same ML problems -- managing computed feature values (what is now called a feature store), applying existing algorithms to train a model from those values, and serving that model by applying it to computed feature values. We saw a different problem.

With our background in the data processing space we identified a critical gap -- no one was looking at the process of going from raw, event-based, data to computed feature values. This meant that users had to choose – use SQL and treat the events as a table, losing important information in the process, or use lower-level data pipeline APIs and worry about all the details. Our experience working on data processing systems at Google and as part of Apache Beam led us to create a compute engine designed for the needs of feature engineering — we called it a feature engine.

We are extremely proud of where Kaskada technology is today. Unlike a feature store, it focuses on computing the features a user described using a simple, declarative language. Unlike existing data processing systems, it delivers on the needs of machine learning – expressing sophisticated, temporal features without leakage, working with raw events without pre-processing, and scalability that just worked for training and serving.

The unique characteristics of Kaskada make it ideal for the time-based event processing required for accurate, real-time machine learning. While we see that ML will always be a great use case for Kaskada, we’ve realized it can be used for so much more.

# Modern, Open-Source Event Processing

When [DataStax acquired Kaskada](https://www.datastax.com/press-release/datastax-acquires-machine-learning-company-kaskada-to-unlock-real-time-ai) a few months ago, we began the process of open-sourcing the core Kaskada technology. In the conversations that followed, we realized that the capabilities of Kaskada that make it ideal for real-time ML – easy to use, high-performance columnar computations over event-based data – also make it great for general event processing. These features include:

1. **Rich, Temporal Operations**: The ability to easily express computations over time beyond windowed aggregations. For instance, when computing training data it was often necessary to compute values at a point in time in the past and combine those with a label value computed at a later point in time. This led to a powerful set of operations for working with time.
2. **Events all the way down**: The ability to run a query both to get all results over time and just the final results. This means that Kaskada operates directly on the events – turning a sequence of events into a sequence of changes, which may be observed directly or materialized to a table. By treating everything as events, the temporal operations are always available and you never need to think about the difference between streams and tables, nor do you need to use different APIs for each.
3. **Modern and easy to use**: Kaskada is built in Rust and uses Apache Arrow for high-performance, columnar computations. It consists of a single binary which makes for easy local and cloud deployments.


This led to the decision to open source Kaskada as a modern, open-source event-processing language and native engine. Machine learning is still a great use case of Kaskada, but we didn’t want the feature engine label to constrain community creativity and innovation. It’s all available today in the [GitHub repository](https://github.com/kaskada-ai/kaskada) under the Apache 2.0 License.

# Why use Kaskada?

Kaskada is for you if…

1. **You want to compute the results of your query over time.**
Operating over time all the way down means that Kaskada makes it easy to compute the result of any query over time.

2. **You want to express temporal computations without writing pages of SQL.**
Kaskada provides a declarative language for event-processing. Because of the focus on temporal computations and composability, it is much easier and shorter than comparable SQL queries.

3. **You want to process events today without setting up other tools.**
The columnar event-processing engine within Kaskada scales to X million events/second running on a single machine. This lets you get started and iterate quickly without becoming an expert in cluster management or big-data tools.


# What’s coming next?

Our first goal was getting the project released. Now that it is, we are excited to see where the project goes!

Some improvements on our mind are shown below. We look forward to hearing your thoughts on what would help you process events.

1.  **Increase extensibility and participate in the larger open-source community.**
    - Introduce extension points for I/O connectors and contribute connectors for a larger set of supported formats.
    - Expose a logical execution plan after the language constructs have been compiled away, so that other executors may be developed using the same parsing and type-checking rules.
    - Introduce extension points for custom schema catalogs, allowing Kaskada queries to be compiled against existing data catalogs.

2. **Align query capabilities with more general, event-processing use cases.**
    - Ability to create composite events from patterns of existing events and subsequently process those composite events (“CEP”).
    - Improvements to the declarative language to reduce surprises, make it more familiar to new users, and make it even easier to express temporal computations over events.

3.  **Continue to improve local performance and usability.**
    - Make it possible to use the engine more easily in a variety of ways – via a command line REPL, via an API, etc.
    - Improve performance and latency of real-time and partitioned execution within the native engine.

# How can I contribute?

Give it a try – [download one of the releases](https://github.com/kaskada-ai/kaskada/releases) and run some computations on your event data. Let us know how it works for you, and what you’d like to see improved!

We’d love to hear what you think - please comment or ask on our [Kaskada GitHub discussions page](https://github.com/kaskada-ai/kaskada/discussions).

Help spread the word – Star and Follow the project on GitHub!

Please file issues, start discussions or join us on GitHub to chat about the project or event-processing in general.