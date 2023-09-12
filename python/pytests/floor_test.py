import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m",
            "1996-12-19T16:39:57,A,5",
            "1996-12-19T16:39:58,B,100.0001",
            "1996-12-19T16:39:59,A,2.50",
            "1996-12-19T16:40:00,A,",
            "1996-12-19T16:40:01,A,0.99",
            "1996-12-19T16:40:02,A,1.01",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_floor(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "floor_m": m.floor()}))
