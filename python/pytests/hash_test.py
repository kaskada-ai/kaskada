import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def string_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "2021-01-01T00:00:00,Ben,hello,",
            "2021-01-01T00:00:00,Ryan,,",
            "2021-01-02T00:00:00,Ryan,world,",
            "2021-01-03T00:00:00,Ben,hi,",
            "2021-01-04T00:00:00,Ben, ,",
            "2021-01-04T00:00:00,Ryan,earth,",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_hash_string(string_source, golden) -> None:
    m = string_source.col("m")
    golden.jsonl(kd.record({"m": m, "hash_m": m.hash()}))


@pytest.fixture(scope="module")
async def integer_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "2021-01-01T00:00:00,Ben,5,",
            "2021-01-01T00:00:00,Ryan,8,",
            "2021-01-02T00:00:00,Ryan,9,",
            "2021-01-03T00:00:00,Ben,8,",
            "2021-01-04T00:00:00,Ben,,",
            "2021-01-04T00:00:00,Ryan,9,",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_hash_integer(integer_source, golden) -> None:
    m = integer_source.col("m")
    golden.jsonl(kd.record({"m": m, "hash_m": m.hash()}))
