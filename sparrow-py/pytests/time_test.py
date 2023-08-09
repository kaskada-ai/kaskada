from datetime import timedelta

import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source() -> kt.sources.CsvSource:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,A,5,10",
            "1996-12-19T16:39:58-08:00,B,24,3",
            "1996-12-19T16:39:59-08:00,A,17,6",
            "1997-01-18T16:40:00-08:00,A,,9",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)


def test_time_add_days(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(kt.record({"time": time, "time_plus_day": time + timedelta(days=1)}))


def test_time_add_hours(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kt.record({"time": time, "time_plus_hours": time + timedelta(hours=1)})
    )


def test_time_add_minutes(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kt.record({"time": time, "time_plus_minutes": time + timedelta(minutes=1)})
    )


def test_time_add_days_and_minutes(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kt.record({"time": time, "time_plus_day": time + timedelta(days=3, minutes=1)})
    )


def test_time_add_seconds(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kt.record({"time": time, "time_plus_seconds": time + timedelta(seconds=5)})
    )
