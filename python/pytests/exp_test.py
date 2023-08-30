import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m",
            "1996-12-19T16:39:57,A,1",
            "1996-12-19T16:39:58,B,1",
            "1996-12-19T16:39:59,A,2",
            "1996-12-19T16:40:00,A,3",
            "1996-12-19T16:40:01,A,4",
            "1996-12-19T16:40:02,A,5",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_exp(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "exp_m": m.exp()}))
