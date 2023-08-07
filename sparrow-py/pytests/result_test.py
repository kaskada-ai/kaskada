import pytest
import sparrow_py as kt


@pytest.fixture
def source_int64() -> kt.sources.CsvSource:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,A,5,10",
            "1996-12-19T16:39:58-08:00,B,24,3",
            "1996-12-19T16:39:59-08:00,A,17,6",
            "1996-12-19T16:40:00-08:00,A,,9",
            "1996-12-19T16:40:01-08:00,A,12,",
            "1996-12-19T16:40:02-08:00,A,,",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)


def test_iter_pandas(golden, source_int64) -> None:
    batches = source_int64.run(row_limit=4, max_batch_size=2).iter_pandas()

    # 4 rows, max 2 per batch = 2 batches
    golden(next(batches))
    golden(next(batches))
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
    golden(await batches.__anext__())
    golden(await batches.__anext__())
    with pytest.raises(StopAsyncIteration):
        await batches.__anext__()
