import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m",
            "2021-01-01T00:00:00,Ben,Hello World",
            "2021-01-02T00:00:00,Ryan,",
            "2021-01-02T00:00:00,Ryan,Hi Earth",
            "2021-01-03T00:00:00,Ben,Hello",
            "2021-01-03T00:00:00,Ben,",
            "2021-01-04T00:00:00,Ryan,hi",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_lower(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "lower_m": m.lower()}))
