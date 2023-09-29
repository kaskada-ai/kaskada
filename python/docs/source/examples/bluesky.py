#!/usr/bin/env python
#
# Bluesky Kaskada Consumer
#
# This script demonstrates the use of Kaskada to consume and compute over
# the BlueSky firehose.

import asyncio
import time

import kaskada as kd
import pyarrow as pa
from atproto import CAR, AtUri, models
from atproto.firehose import (
    AsyncFirehoseSubscribeReposClient,
    parse_subscribe_repos_message,
)
from atproto.xrpc_client.models import get_or_create, ids, is_record_type


async def main():
    # Initialize the Kaskada session so we can use it for stream processing
    kd.init_session()

    # Create the BlueSky (At protocol) client.
    # The firehose doesn't (currently) require authentication.
    at_client = AsyncFirehoseSubscribeReposClient()

    # [start_setup]
    # Setup the data source.
    # This defintes (most of) the schema of the events we'll receive,
    # and tells Kaskada which fields to use for time and initial entity.
    #
    # We'll push events into this source as they arrive in real-time.
    posts = kd.sources.PyDict(
        rows=[],
        schema=pa.schema(
            [
                pa.field(
                    "record",
                    pa.struct(
                        [
                            pa.field("created_at", pa.string()),
                            pa.field("text", pa.string()),
                            # pa.field("embed", pa.struct([...])),
                            pa.field("entities", pa.string()),
                            # pa.field("facets", pa.list_(...)),
                            pa.field("labels", pa.float64()),
                            pa.field("langs", pa.list_(pa.string())),
                            # pa.field("reply", pa.struct([...])),
                            pa.field("py_type", pa.string()),
                        ]
                    ),
                ),
                pa.field("uri", pa.string()),
                pa.field("cid", pa.string()),
                pa.field("author", pa.string()),
                pa.field("ts", pa.float64()),
            ]
        ),
        time_column="ts",
        key_column="author",
        time_unit="s",
    )
    # [end_setup]

    # [start_incoming]
    # Handler for newly-arrived messages from BlueSky.
    async def receive_at(message) -> None:
        # Extract the contents of the message and bail if it's not a "commit"
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        # Get the operations included in the message
        ops = _get_ops_by_type(commit)
        for new_post in ops["posts"]["created"]:
            # The atproto library's use of schemas is sort of confusing
            # This isn't always the expected type and I'm not sure why...
            if not isinstance(new_post["record"], models.app.bsky.feed.post.Main):
                continue

            # The parsing produces a hot mess of incompatible types, so we build
            # a dict from scratch to simplify.
            posts.add_rows(
                {
                    "record": dict(new_post["record"]),
                    "uri": new_post["uri"],
                    "cid": new_post["cid"],
                    "author": new_post["author"],
                    "ts": time.time(),
                }
            )

    # [end_incoming]

    # [start_result]
    # Handler for values emitted by Kaskada.
    async def receive_outputs():
        # We'll perform a very simple aggregation - key by language and count.
        posts_by_first_lang = posts.with_key(posts.col("record").col("langs").index(0))

        # Consume outputs as they're generated and print to STDOUT.
        async for row in posts_by_first_lang.count().run_iter(kind="row", mode="live"):
            print(f"{row['_key']} has posted {row['result']} times since startup")

    # [end_result]

    # [start_run]
    # Kickoff the two async processes concurrently.
    await asyncio.gather(at_client.start(receive_at), receive_outputs())
    # [end_run]


# Copied from https://raw.githubusercontent.com/MarshalX/atproto/main/examples/firehose/process_commits.py
def _get_ops_by_type(  # noqa: C901, E302
    commit: models.ComAtprotoSyncSubscribeRepos.Commit,
) -> dict:  # noqa: C901, E302
    operation_by_type = {
        "posts": {"created": [], "deleted": []},
        "reposts": {"created": [], "deleted": []},
        "likes": {"created": [], "deleted": []},
        "follows": {"created": [], "deleted": []},
    }

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f"at://{commit.repo}/{op.path}")

        if op.action == "update":
            # not supported yet
            continue

        if op.action == "create":
            if not op.cid:
                continue

            create_info = {"uri": str(uri), "cid": str(op.cid), "author": commit.repo}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = get_or_create(record_raw_data, strict=False)
            if uri.collection == ids.AppBskyFeedLike and is_record_type(
                record, ids.AppBskyFeedLike
            ):
                operation_by_type["likes"]["created"].append(
                    {"record": record, **create_info}
                )
            elif uri.collection == ids.AppBskyFeedPost and is_record_type(
                record, ids.AppBskyFeedPost
            ):
                operation_by_type["posts"]["created"].append(
                    {"record": record, **create_info}
                )
            elif uri.collection == ids.AppBskyFeedRepost and is_record_type(
                record, ids.AppBskyFeedRepost
            ):
                operation_by_type["reposts"]["created"].append(
                    {"record": record, **create_info}
                )
            elif uri.collection == ids.AppBskyGraphFollow and is_record_type(
                record, ids.AppBskyGraphFollow
            ):
                operation_by_type["follows"]["created"].append(
                    {"record": record, **create_info}
                )

        if op.action == "delete":
            if uri.collection == ids.AppBskyFeedLike:
                operation_by_type["likes"]["deleted"].append({"uri": str(uri)})
            elif uri.collection == ids.AppBskyFeedPost:
                operation_by_type["posts"]["deleted"].append({"uri": str(uri)})
            elif uri.collection == ids.AppBskyFeedRepost:
                operation_by_type["reposts"]["deleted"].append({"uri": str(uri)})
            elif uri.collection == ids.AppBskyGraphFollow:
                operation_by_type["follows"]["deleted"].append({"uri": str(uri)})

    return operation_by_type


if __name__ == "__main__":
    asyncio.run(main())
