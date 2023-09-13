# Bluesky Firehose Example

Bluesky is a "distributed social network" that aims to improve on some of the perceived shortcomings of X (nee Twitter). 
Bluesky uses a distributed protocol name the [AT Protocol](https://atproto.com/) to exchange messages between users, and provides a "firehose" delivering every message sent over the protocol in real-time. 

In this example, we'll show how you can receive and process the firehose using Kaskada.

You can see the full example in the file [bluesky.py](./bluesky.py)

## Setup the event data source

Before we can receive events from Bluesky, we need to create a data source to tell Kaskada how to handle the events. 
We'll provide a schema and configure the time and entity fields.

.. literalinclude:: bluesky.py
    :language: python
    :linenos: 24-50

## Define the incoming event handler

The `atproto` python library takes care of requesting and receiving events from Bluesky, all you need to do is create a handler to configure what to do with each event. 
This handler parses the message to find [Commit](https://atproto.com/specs/repository#commit-objects) events.
For each Commit, we'll parse out any [Post](https://atproto.com/blog/create-post#post-record-structure) messages. 
Finally we do some schema munging to get the Post into the event format we described when creating the data source.

.. literalinclude:: bluesky.py
    :language: python
    :linenos: 53-77

## Construct a real-time query and result handler

Now we can use Kaskada to transform the events as they arrive.
First we'll use `with_key` to regroup events by language, then we'll apply a simple `count` aggregation.
Finally, we create a handler for the transformed results - here just printing them out.

.. literalinclude:: bluesky.py
    :language: python
    :linenos: 79-87

## Final touches

Now we just need to kick it all off by calling `asyncio.gather` on the two handler coroutines. This kicks off all the async processing.

.. literalinclude:: bluesky.py
    :language: python
    :linenos: 89-90

Try running it yourself and trying differente transformations!

```bash
python bluesky.py
```