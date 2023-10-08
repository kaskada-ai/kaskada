#!/usr/bin/env python
#
# Reddit Kaskada Consumer
#
# This script demonstrates the use of Kaskada to consume and compute over
# Reddit updates.

import asyncio
import os
import time

import asyncpraw
import kaskada as kd
import pyarrow as pa


async def main():
    # Initialize the Kaskada session so we can use it for stream processing
    kd.init_session()

    # Create the Reddit client.
    reddit = asyncpraw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT", "kaskada-demo"),
    )

    # [start_setup]
    # Setup the data source.
    # This defintes (most of) the schema of the events we'll receive,
    # and tells Kaskada which fields to use for time and initial entity.
    #
    # We'll push events into this source as they arrive in real-time.
    comments = kd.sources.PyDict(
        schema=pa.schema(
            [
                pa.field("author", pa.string()),
                pa.field("body", pa.string()),
                pa.field("permalink", pa.string()),
                pa.field("submission_id", pa.string()),
                pa.field("subreddit", pa.string()),
                pa.field("ts", pa.float64()),
            ]
        ),
        time_column="ts",
        key_column="submission_id",
        time_unit="s",
    )
    # [end_setup]

    # [start_incoming]
    # Handler to receive new comments as they're created
    async def receive_comments():
        # Creat the subreddit handle
        sr = await reddit.subreddit(os.getenv("SUBREDDIT", "all"))

        # Consume the stream of new comments
        async for comment in sr.stream.comments():
            # Add each comment to the Kaskada data source
            await comments.add_rows(
                {
                    "author": comment.author.name,
                    "body": comment.body,
                    "permalink": comment.permalink,
                    "submission_id": comment.submission.id,
                    "subreddit_id": comment.subreddit.display_name,
                    "ts": time.time(),
                }
            )

    # [end_incoming]

    # [start_result]
    # Handler for values emitted by Kaskada.
    async def receive_outputs():
        # We'll perform a very simple aggregation - key by author and count.
        comments_by_author = comments.with_key(comments.col("author"))

        # Consume outputs as they're generated and print to STDOUT.
        async for row in comments_by_author.count().run_iter(kind="row", mode="live"):
            print(f"{row['_key']} has posted {row['result']} times since startup")

    # [end_result]

    # [start_run]
    # Kickoff the two async processes concurrently.
    await asyncio.gather(receive_comments(), receive_outputs())
    # [end_run]


if __name__ == "__main__":
    asyncio.run(main())
