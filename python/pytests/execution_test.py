from datetime import datetime
from typing import AsyncIterator, Iterator

import kaskada as kd
import pandas as pd
import pytest


@pytest.fixture
def source_int64() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1996-12-19T16:40:00,A,,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_iter_pandas(golden, source_int64) -> None:
    batches = source_int64.run_iter(row_limit=4, max_batch_size=2)

    # 4 rows, max 2 per batch = 2 batches
    golden.jsonl(next(batches))
    golden.jsonl(next(batches))
    with pytest.raises(StopIteration):
        next(batches)


def test_iter_rows(golden, source_int64) -> None:
    results: Iterator[dict] = source_int64.run_iter("row", row_limit=2)
    assert next(results)["m"] == 5
    assert next(results)["m"] == 24
    with pytest.raises(StopIteration):
        next(results)


@pytest.mark.asyncio
async def test_iter_pandas_async(golden, source_int64) -> None:
    batches: AsyncIterator[pd.DataFrame] = source_int64.run_iter(
        row_limit=4, max_batch_size=2
    )

    # 4 rows, max 2 per batch = 2 batches.

    # We could test using `await anext(batches)`, but that wasn't introduced
    # until Python 3.10. Since everything else works in 3.8 and 3.9, we just
    # call `__anext__` directly.
    golden.jsonl(await batches.__anext__())
    golden.jsonl(await batches.__anext__())
    with pytest.raises(StopAsyncIteration):
        await batches.__anext__()


@pytest.mark.asyncio
async def test_iter_pandas_async_live(golden, source_int64) -> None:
    data2 = "\n".join(
        [
            "time,key,m,n",
            "1996-12-20T16:39:57,A,5,10",
            "1996-12-20T16:39:58,B,24,3",
            "1996-12-20T16:39:59,A,17,6",
            "1996-12-20T16:40:00,C,,9",
            "1996-12-20T16:40:01,A,12,",
            "1996-12-20T16:40:02,A,,",
        ]
    )

    execution = source_int64.run_iter(mode="live")

    # Await the first batch.
    golden.jsonl(await execution.__anext__())

    # Add data and await the second batch.
    source_int64.add_string(data2)
    golden.jsonl(await execution.__anext__())

    execution.stop()
    with pytest.raises(StopAsyncIteration):
        print(await execution.__anext__())


def test_snapshot(golden, source_int64) -> None:
    query = source_int64.col("m").sum()
    golden.jsonl(query.to_pandas(kd.results.Snapshot()))
    golden.jsonl(
        query.to_pandas(
            kd.results.Snapshot(
                changed_since=datetime.fromisoformat("1996-12-19T16:39:59+00:00")
            )
        )
    )
    golden.jsonl(
        query.to_pandas(
            kd.results.Snapshot(at=datetime.fromisoformat("1996-12-20T12:00:00+00:00"))
        )
    )


def test_history(golden, source_int64) -> None:
    query = source_int64.col("m").sum()
    golden.jsonl(query.to_pandas(kd.results.History()))
    golden.jsonl(
        query.to_pandas(
            kd.results.History(
                since=datetime.fromisoformat("1996-12-19T16:39:59+00:00")
            )
        )
    )
    golden.jsonl(
        query.to_pandas(
            kd.results.History(
                until=datetime.fromisoformat("1996-12-20T12:00:00+00:00")
            )
        )
    )
    golden.jsonl(
        query.to_pandas(
            kd.results.History(
                since=datetime.fromisoformat("1996-12-19T16:39:59+00:00"),
                until=datetime.fromisoformat("1996-12-20T12:00:00+00:00"),
            )
        )
    )
