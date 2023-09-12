import kaskada as kd
import pyarrow as pa
import pytest


@pytest.fixture
async def source() -> kd.sources.CsvString:
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
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_seconds_since_previous(golden, source) -> None:
    t = source.col("time")
    golden.jsonl(
        kd.record(
            {
                "seconds_since": t.seconds_since_previous().cast(pa.int64()),
                "seconds_since_1": t.seconds_since_previous(1).cast(pa.int64()),
                "seconds_since_2": t.seconds_since_previous(2).cast(pa.int64()),
            }
        )
    )
