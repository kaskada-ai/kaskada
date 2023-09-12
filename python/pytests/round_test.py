import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m",
            "2021-01-01T00:00:00,A,5.7",
            "2021-01-01T00:00:00,A,6.3",
            "2021-01-02T00:00:00,B,",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_round(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "round_m": m.round()}))
