import kaskada as kd

import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
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
    return kd.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_clamp_min_max(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "clamped_m": m.clamp(min=5, max=100)}))


def test_clamp_min(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "clamped_min": m.clamp(min=5)}))


def test_clamp_max(source, golden) -> None:
    m = source.col("m")
    golden.jsonl(kd.record({"m": m, "clamped_min": m.clamp(max=100)}))
