import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n,o",
            "1996-12-19T16:39:57,A,5,10,15",
            "1996-12-19T16:39:58,B,24,3,15",
            "1996-12-19T16:39:59,A,17,6,15",
            "1996-12-19T16:40:00,A,,9,15",
            "1996-12-19T16:40:01,A,12,,15",
            "1996-12-19T16:40:02,A,,,15",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_coalesce(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "n": n, "coalesced_val": m.coalesce(n)}))


def test_coalesce_three(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    o = source.col("o")
    golden.jsonl(kd.record({"m": m, "n": n, "o": o, "coalesced_val": m.coalesce(n, o)}))
