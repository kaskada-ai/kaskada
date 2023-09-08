---
html_theme.sidebar_secondary.remove: true
sd_hide_title: true
---

# Real-Time AI without the fuss.

<div class="px-4 py-5 my-5 text-center">
    <img class="d-block mx-auto mb-4 only-light" src="_static/kaskada-positive.svg" alt="" width="50%">
    <img class="d-block mx-auto mb-4 only-dark" src="_static/kaskada-negative.svg" alt="" width="50%">
    <h1 class="display-5 fw-bold">Real-Time AI without the fuss.</h1>
    <div class="col-lg-7 mx-auto">
      <p class="lead mb-4">Kaskada is a next-generation streaming engine that connects AI models to real-time & historical data.
      </p>
    </div>
</div>

## Kaskada completes the Real-Time AI stack, providing...

```{gallery-grid}
:grid-columns: 1 2 2 3

- header: "{fas}`timeline;pst-color-primary` Real-time Aggregation"
  content: "Precompute model inputs from streaming data with robust data connectors, transformations & aggregations."
- header: "{fas}`binoculars;pst-color-primary` Event Detection"
  content: "Trigger pro-active AI behaviors by identifying important activities, as they happen."
- header: "{fas}`backward;pst-color-primary` History Replay"
  content: "Backtest and fine-tune from historical data using per-example time travel and point-in-time joins."
```


## Real-time AI in minutes

Connect and compute over databases, streaming data, _and_ data loaded dynamically using Python..
Kaskada is seamlessly integrated with Python's ecosystem of AI/ML tooling so you can load data, process it, train and serve models all in the same place.

There's no infrastructure to provision (and no JVM hiding under the covers), so you can jump right in - check out the [Quick Start](./guide/quickstart.md).


## Built for scale and reliability

Implemented in [Rust](https://www.rust-lang.org/) using [Apache Arrow](https://arrow.apache.org/), Kaskada's compute engine uses columnar data to efficiently execute large historic and high-throughput streaming queries.
Every operation in Kaskada is implemented incrementally, allowing automatic recovery if the process is terminated or killed.

With Kaskada, most jobs are fast enough to run locally, so it's easy to build and test your real-time queries.
As your needs grow, Kaskada's cloud-native design and support for partitioned execution gives you the volume and throughput you need to scale.
Kaskada was built by core contributors to [Apache Beam](https://beam.apache.org/), [Google Cloud Dataflow](https://cloud.google.com/dataflow), and [Apache Cassandra](https://cassandra.apache.org/), and is under active development

* * *

## Example Real-Time App: BeepGPT

[BeepGPT](https://github.com/kaskada-ai/beep-gpt/tree/main) keeps you in the loop without disturbing your focus. Its personalized, intelligent AI continuously monitors your Slack workspace, alerting you to important conversations and freeing you to concentrate on what’s most important.

The core of BeepGPT's real-time processing requires only a few lines of code using Kaskada:

```python
import kaskada as kd
kd.init_session()

# Bootstrap from historical data
messages = kd.sources.PyDict(
    rows = pyarrow.parquet.read_table("./messages.parquet")
        .to_pylist(),
    time_column = "ts",
    key_column = "channel",
)

# Send each Slack message to Kaskada
def handle_message(client, req):
    messages.add_rows(req.payload["event"])
slack.socket_mode_request_listeners.append(handle_message)
slack.connect()

# Aggregate multiple messages into a "conversation"
conversations = ( messages
    .select("user", "text")
    .collect(max=20)
)

# Handle each conversation as it occurs
async for row in conversations.run_iter(mode='live'):

    # Use a pre-trained model to identify interested users
    prompt = "\n\n".join([f' {msg["user"]} --> {msg["text"]} ' for msg in row["result"]])
    res = openai.Completion.create(
        model="davinci:ft-personal:coversation-users-full-kaskada-2023-08-05-14-25-30",
        prompt=prompt + "\n\n###\n\n",
        logprobs=5,
        max_tokens=1,
        stop=" end",
        temperature=0.25,
    )

    # Notify interested users using the Slack API
    for user_id in interested_users(res):
        notify_user(row, user_id)
```

For more details, check out the [BeepGPT Github project](https://github.com/kaskada-ai/beep-gpt).

* * *

## Get Started

Getting started with Kaskda is a `pip install kaskada` away.
Check out the [Quick Start](./guide/quickstart.md) now!

```{toctree}
:hidden:
:maxdepth: 3

guide/index
examples/index
reference/index
blog/index
```