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


def test_math_int64(golden, source_int64) -> None:
    m = source_int64["m"]
    n = source_int64["n"]
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
