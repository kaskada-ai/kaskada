# Reddit Live Example

In this example, we'll show how you can receive and process Reddit comments using Kaskada.

You can see the full example in the file [reddit.py](https://github.com/kaskada-ai/kaskada/blob/main/python/docs/source/examples/reddit.py).

## Setup Reddit credentials

Follow Reddit's [First Steps](https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example#first-steps) guide to create an App and obtain a client ID and secret.
The "script" type application is sufficient for this example.

## Setup the event data source

Before we can receive events from Reddit, we need to create a data source to tell Kaskada how to handle the events.
We'll provide a schema and configure the time and entity fields.

```{literalinclude} reddit.py
:language: python
:start-after: "[start_setup]"
:end-before: "[end_setup]"
:linenos:
:lineno-match:
:dedent: 4
```

## Define the incoming event handler

The `asyncpraw` python library takes care of requesting and receiving events from Reddit, all you need to do is create a handler to configure what to do with each event.
This handler converts [Comment](https://praw.readthedocs.io/en/stable/code_overview/models/comment.html#praw.models.Comment) messages into a dict, and passes the dict to Kaskada.

```{literalinclude} reddit.py
:language: python
:start-after: "[start_incoming]"
:end-before: "[end_incoming]"
:linenos:
:lineno-match:
:dedent: 4
```

## Construct a real-time query and result handler

Now we can use Kaskada to transform the events as they arrive.
First we'll use `with_key` to regroup events by author, then we'll apply a simple `count` aggregation.
Finally, we create a handler for the transformed results - here just printing them out.


```{literalinclude} reddit.py
:language: python
:start-after: "[start_result]"
:end-before: "[end_result]"
:linenos:
:lineno-match:
:dedent: 4
```

## Final touches

Now we just need to kick it all off by calling `asyncio.gather` on the two handler coroutines. This kicks off all the async processing.

```{literalinclude} reddit.py
:start-after: "[start_run]"
:end-before: "[end_run]"
:language: python
:linenos:
:lineno-match:
:dedent: 4
```

Try running it yourself and playing different transformations!

```bash
python reddit.py
```
