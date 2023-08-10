from datetime import timedelta

import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source() -> kt.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1997-01-18T16:40:00,A,,9",
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

def test_shift_collect(source, golden) -> None:
    time = source.col("time")

    base = kt.record(
        {
            "time": time,
            #LAST shouldn't be needed to make it continuous.
            "ms": source.col("m").collect(max=10).last(),
        }
    )

    golden.jsonl(
        base.extend({
            "m": source.col("m"),
            "shift_by_1_s": base.shift_by(timedelta(seconds=1)),
            "shift_by_1_m": base.shift_by(timedelta(minutes=1)),
        })
    )