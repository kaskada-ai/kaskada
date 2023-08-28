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


def test_shift_by_timedelta(source, golden) -> None:
    time = source.col("time")
    golden.jsonl(
        kd.record(
            {
                "time": time,
                "shift_by_1_s": time.shift_by(timedelta(seconds=1)),
                "shift_by_1_m": time.shift_by(timedelta(minutes=1)),
            }
        )
    )


def test_shift_collect(source, golden) -> None:
    golden.jsonl(
        source.record(
            lambda input: {
                "time": input.col("time"),
                "ms": input.col("m").collect(max=10),
                "m": input.col("m"),
            }
        )
        # Currently, the Pandas comparison method being used doesn't handle
        # date-time like fields nested within a list. So we expand things out.
        #
        # TODO: Improve the golden testing so this isn't necessary.
        .extend(
            lambda base: {
                "shift_by_1_s_time": base.shift_by(timedelta(seconds=1)).col("time"),
                "shift_by_1_s_ms": base.shift_by(timedelta(seconds=1)).col("ms"),
                "shift_by_1_m_time": base.shift_by(timedelta(minutes=1)).col("time"),
                "shift_by_1_m_ms": base.shift_by(timedelta(minutes=1)).col("ms"),
            }
        )
    )
