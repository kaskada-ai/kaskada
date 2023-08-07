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


def test_iterate_pandas(golden, source_int64) -> None:
    results = source_int64.run(row_limit=4, max_batch_size=2).iter_pandas()

    # 4 rows, max 2 per batch = 2 batches
    golden.jsonl(next(results))
    golden.jsonl(next(results))
    with pytest.raises(StopIteration):
        next(results)


def test_iterate_rows(golden, source_int64) -> None:
    results = source_int64.run(row_limit=2).iter_rows()
    assert next(results)["m"] == 5
    assert next(results)["m"] == 24
    with pytest.raises(StopIteration):
        next(results)
