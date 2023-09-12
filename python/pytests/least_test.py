import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,a,b",
            "2021-01-01T00:00:00,A,5.7,1.2",
            "2021-01-02T00:00:00,A,6.3,0.4",
            "2021-01-03T00:00:00,B,,3.7",
            "2021-01-04T00:00:00,A,13.2,",
            "2021-01-05T00:00:00,A,2,5.4",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_least(source, golden) -> None:
    a = source.col("a")
    b = source.col("b")
    golden.jsonl(kd.record({"a": a, "b": b, "a_least_b": a.least(b)}))
