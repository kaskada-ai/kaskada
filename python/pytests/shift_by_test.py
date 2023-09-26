from datetime import timedelta

import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1997-01-18T16:40:00,A,,9",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


@pytest.fixture(scope="module")
async def filter_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "thread_ts,ts,channel",
            "null,1691762610.0,Project",
            "null,1691762620.0,Project",
            "null,1691762630.0,Project",
            "1691762650.0,1691762650.0,Project",
            "1691762650.0,1691762660.0,Project",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="ts", key_column="channel", time_unit="s"
    )


async def test_shift_by_timedelta(source, golden) -> None:
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


async def test_shift_collect(source, golden) -> None:
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


# Regression test for https://github.com/kaskada-ai/kaskada/issues/726
async def test_filter_to_shift(filter_source, golden) -> None:
    messages = filter_source.filter(filter_source.col("thread_ts").is_null())
    messages = messages.shift_by(timedelta(seconds=5))
    golden.jsonl(messages)
