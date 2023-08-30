import datetime as datetime

import kaskada as kd
import pyarrow as pa
import pytest


@pytest.fixture
def source() -> kd.sources.CsvString:
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
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_seconds_since(golden, source) -> None:
    t1 = source.col("time")
    t2 = source.col("t")
    golden.jsonl(
        kd.record(
            {
                "t1": t1,
                "t2": t2,
                "seconds_since_t1": t2.seconds_since(t1).cast(pa.int64()),
                "seconds_since_t2": t1.seconds_since(t2).cast(pa.int64()),
            }
        )
    )


def test_seconds_since_datetime(golden, source) -> None:
    t = source.col("time")
    dt = datetime.datetime(1996, 12, 19, 16, 39, 50, tzinfo=datetime.timezone.utc)
    golden.jsonl(
        kd.record(
            {"t1": t, "seconds_since_literal": t.seconds_since(dt).cast(pa.int64())}
        )
    )
