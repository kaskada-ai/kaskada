from datetime import timedelta

import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source() -> kt.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,A,5,10",
            "1996-12-19T16:39:58-08:00,B,24,3",
            "1996-12-19T16:39:59-08:00,A,17,6",
            "1997-01-18T16:40:00-08:00,A,,9",
        ]
    )
    return kt.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_shift_by_timedelta(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kt.record(
            {
                "time": time,
                "shift_by_1_s": time.shift_by(timedelta(seconds=1)),
                "shift_by_1_m": time.shift_by(timedelta(minutes=1)),
            }
        )
    )
