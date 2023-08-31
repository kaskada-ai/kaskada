from datetime import timedelta

import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1997-01-18T16:40:00,A,,9",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_time_of_point(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "time_of_m": m.time(),
                "n": n,
                "time_of_n": n.time(),
            }
        )
    )


def test_time_add_days(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(kd.record({"time": time, "time_plus_day": time + timedelta(days=1)}))


def test_time_add_hours(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kd.record({"time": time, "time_plus_hours": time + timedelta(hours=1)})
    )


def test_time_add_minutes(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kd.record({"time": time, "time_plus_minutes": time + timedelta(minutes=1)})
    )


def test_time_add_days_and_minutes(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kd.record({"time": time, "time_plus_day": time + timedelta(days=3, minutes=1)})
    )


def test_time_add_seconds(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kd.record({"time": time, "time_plus_seconds": time + timedelta(seconds=5)})
    )


def test_comparing_timedeltas(source, golden) -> None:
    time = source.col("time")
    seconds_since = time.seconds_since_previous()
    td = timedelta(seconds=1)
    golden.jsonl(
        kd.record(({"time": time, "seconds_since": seconds_since, "is_1_s": seconds_since.eq(td)}))
    )


