import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n,default",
            "1996-12-19T16:39:57,A,5,10,-1",
            "1996-12-19T16:39:58,B,24,3,-1",
            "1996-12-19T16:39:59,A,17,6,-1",
            "1996-12-19T16:40:00,A,,9,-1",
            "1996-12-19T16:40:01,A,12,,-1",
            "1996-12-19T16:40:02,A,,,-1",
        ]
    )
    return kd.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_else_(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    default = source.col("default")
    condition_m = m > 15
    condition_n = n > 5
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "condition_m": condition_m,
                "if_else_m": m.if_(condition_m).else_(default),
                "n": n,
                "condition_n": condition_n,
                "if_n": n.if_(condition_n).else_(default),
            }
        )
    )
