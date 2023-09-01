from datetime import datetime, timedelta

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


@pytest.mark.skip(reason="shift to literal not supported")
def test_shift_to_datetime(source, golden) -> None:
    time = source.col("time")
    shift_to_datetime = datetime(1996, 12, 25, 0, 0, 0)
    golden.jsonl(
        kd.record(
            {"time": time, "shift_to_time_plus_1_day": time.shift_to(shift_to_datetime)}
        )
    )


def test_shift_to_column(source, golden) -> None:
    time = source.col("time")
    shift_by_timedelta = timedelta(seconds=10)
    golden.jsonl(
        kd.record(
            {
                "time": time,
                "time_plus_seconds": time.shift_to(time.time() + shift_by_timedelta),
            }
        )
    )
