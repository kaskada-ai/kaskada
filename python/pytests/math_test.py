import pytest

import kaskada as kt


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


def test_math_int64(golden, source_int64) -> None:
    m = source_int64.col("m")
    n = source_int64.col("n")
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "n": n,
                "add": m + n,
                "sub": m - n,
            }
        )
    )
