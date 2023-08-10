import pytest

import sparrow_py as kt


@pytest.fixture
def source_int64() -> kt.sources.CsvString:
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
    return kt.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_iter_pandas(golden, source_int64) -> None:
    batches = source_int64.run(row_limit=4, max_batch_size=2).iter_pandas()

    # 4 rows, max 2 per batch = 2 batches
    golden.jsonl(next(batches))
    golden.jsonl(next(batches))
    with pytest.raises(StopIteration):
        next(batches)


def test_iter_rows(golden, source_int64) -> None:
    results = source_int64.run(row_limit=2).iter_rows()
    assert next(results)["m"] == 5
    assert next(results)["m"] == 24
    with pytest.raises(StopIteration):
        next(results)


@pytest.mark.asyncio
async def test_iter_pandas_async(golden, source_int64) -> None:
    batches = source_int64.run(row_limit=4, max_batch_size=2).iter_pandas_async()

    # 4 rows, max 2 per batch = 2 batches.

    # We could test using `await anext(batches)`, but that wasn't introduced
    # until Python 3.10. Since everything else works in 3.8 and 3.9, we just
    # call `__anext__` directly.
    golden.jsonl(await batches.__anext__())
    golden.jsonl(await batches.__anext__())
    with pytest.raises(StopAsyncIteration):
        await batches.__anext__()


@pytest.mark.asyncio
async def test_iter_pandas_async_materialize(golden, source_int64) -> None:
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

    execution = source_int64.run(materialize=True)
    batches = execution.iter_pandas_async()

    # Await the first batch.
    golden.jsonl(await batches.__anext__())

    # Add data and await the second batch.
    source_int64.add_string(data2)
    golden.jsonl(await batches.__anext__())

    execution.stop()
    with pytest.raises(StopAsyncIteration):
        print(await batches.__anext__())
