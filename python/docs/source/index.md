---
hide-toc: true
html_theme.sidebar_secondary.remove: true
title: Kaskada Timestreams
---

<div class="px-4 py-5 my-5 text-center">
    <img class="d-block mx-auto mb-4" src="_static/kaskada.svg" alt="" width="auto">
    <h1 class="display-5 fw-bold">Event-processing for AI applications.</h1>
    <div class="col-lg-7 mx-auto">
      <p class="lead mb-4">Next-generation, real-time and historic event processing.
      </p>
    </div>
</div>

```{gallery-grid}
:grid-columns: 1 2 2 3

- header: "{fas}`timeline;pst-color-primary` Real-time processing for all"
  content: "Quickly process events so you can respond in real-time."
  link: ".#stream"
- header: "{fab}`python;pst-color-primary` Python-native"
  content: "Use Python so you can load data, process it, and train and serve models from one place."
  link: ".#python"
- header: "{fas}`gauge-high;pst-color-primary` Get started immediately"
  content: "No infrastructure to provision let's you jump right in."
  link: ".#get-started"

- header: "{fas}`fast-forward;pst-color-primary` Real-time, Batch and Streaming"
  content: "Execute large-historic queries or materialize in real-time. Or both."
  link: ".#real-time-and-historic"
- header: "{fas}`rocket;pst-color-primary` Local, Remote and Distributed"
  content: "Develop and test locally. Deploy to Docker, K8s or a service for production."
  link: ".#local-and-distributed"
- header: "{fas}`backward;pst-color-primary` Time-travel"
  content: "Generate training examples from the past to predict the future."
  link: ".#time-travel"
```

* * *

(stream)=
# Real-time event-processing

Kaskada is built on Apache Arrow, providing an efficient, columnar representation of data.
The same approach is at the core of many analytic databases as well as Pandas and Polars.

Kaskada goes beyond the columnar representation, by introduce a Timestream -- a columnar representation of events, ordered by time and grouped by key.
This representation is a perfect fit for all kinds of events, modern event streams as well as events stored in a database.
Specializing for Timestreams allows Kaskada to optimize temporal queries and execute them much faster.

(python)=
# Python-native

Connect to existing data in streams or databases, or load data using Python.
Wherever your events are stored, Kaskada can help you process them.

Build temporal queries and process the results using Python.
Connect straight to your visualizations, dashboards or machine learning systems.

Kaskada lets you do it all in one place.

(get_started)=
# Get Started

With no infrastructure to deploy, get started processing events immediately.
Check out the [Quick Start](quickstart) now!

(local-and-distributed)=
# Local, Remote and Distributed

Fast enough to run locally, Kaskada makes it easy to build and test your real-time queries.

Built for the cloud and supporting partitioned and distributed execution, Kaskada scales to the volume and throughput you need.


(real_time_and_historic)=
# Real-time and Historic

Process events in real-time as they arrive.
Backfill materializations by starting with history and switching to the stream.

(time-travel)=
# Time Travel
Compute temporal joins at the correct times, without risk of leakage.

```{toctree}
:hidden:
:maxdepth: 3

why
tour
quickstart
examples/index
guide/index
```

```{toctree}
:caption: Reference
:hidden:
:maxdepth: 3

reference/timestream/index
reference/windows
reference/sources
reference/results
```