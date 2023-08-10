import datetime as datetime

import pytest
import sparrow_py as kt


@pytest.fixture
def source() -> kt.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n,t",
            "1996-12-19T16:39:57,A,5,10,1996-12-19T16:42:57",
            "1996-12-19T16:39:58,B,24,3,1996-12-19T16:39:59",
            "1996-12-19T16:39:59,A,17,6,",
            "1996-12-19T16:40:00,A,,9,1996-12-19T16:41:00",
            "1996-12-19T16:40:01,A,12,,1996-12-19T16:42:01",
            "1996-12-19T16:40:02,A,,,1996-12-19T16:43:02",
        ]
    )
    return kt.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_seconds_since(golden, source) -> None:
    t1 = source.col("time")
    t2 = source.col("t")
    golden.jsonl(
        kt.record(
            {
                "t1": t1,
                "t2": t2,
                "seconds_since_t1": t2.seconds_since(t1),
                "seconds_since_t2": t1.seconds_since(t2),
            }
        )
    )


def test_seconds_since_literal(golden, source) -> None:
    t = source.col("time")
    dt = datetime.datetime(1996, 12, 19, 16, 39, 50, tzinfo=datetime.timezone.utc)
    golden.jsonl(
        kt.record(
            {
                "t1": t,
                "seconds_since_literal": t.seconds_since(dt),
            }
        )
    )
