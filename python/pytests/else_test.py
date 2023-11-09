import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n,default",
            "1996-12-19T16:39:57,A,5,10,-1",
            "1996-12-19T16:39:58,B,24,3,-1",
            "1996-12-19T16:39:59,A,17,6,-1",
            "1996-12-19T16:40:00,A,,9,-1",
            "1996-12-19T16:40:01,A,12,,-1",
            "1996-12-19T16:40:02,A,,,-1",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_else_(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    default = source.col("default")
    condition_m = m > 15
    condition_n = n > 5
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "condition_m": condition_m,
                "if_else_m": m.if_(condition_m).else_(default),
                "n": n,
                "condition_n": condition_n,
                "if_n": n.if_(condition_n).else_(default),
            }
        )
    )


@pytest.fixture(scope="module")
async def record_source() -> kd.sources.JsonlString:
    content = "\n".join(
        [
            """{"time":"1996-12-19T16:39:57","key":"A","override": {"test":"override_val"}}""",
            """{"time":"1996-12-19T16:39:58","key":"A","default_record":{"test":"default"}}""",
        ]
    )
    return await kd.sources.JsonlString.create(
        content, time_column="time", key_column="key"
    )


async def test_else_debug(record_source, golden) -> None:
    default_record = record_source.col("default_record")
    override_column = record_source.col("override")
    golden.jsonl(
        kd.record(
            {
                "default_record": default_record,
                "overide": override_column,
                "override_else_default": override_column.else_(default_record),
            }
        )
    )


@pytest.fixture(scope="module")
def record_source_slack() -> kd.sources.JsonlString:
    content = "\n".join(
        [
            """{"text":"Thread 1","user":"UCZ4","time":1,"thread_ts":1,"key":"dev"}""",
            """{"text":"Thread 2","user":"U016","time":2,"thread_ts":1,"key":"dev"}""",
            """{"text":"Msg 1","user":"U016","time":3,"thread_ts":null,"key":"dev"}""",
            """{"text":"Msg 2","user":"U016","time":4,"thread_ts":null,"key":"dev"}""",
        ]
    )
    return kd.sources.JsonlString(
        content, time_column_name="time", key_column_name="key"
    )


def test_else_unordered_record(record_source_slack, golden) -> None:
    threads = record_source_slack.filter(record_source_slack.col("thread_ts").is_not_null())
    non_threads = record_source_slack.filter(record_source_slack.col("thread_ts").is_null())

    # this call re-orders the columns in the non_threads timestream
    # and causes the bug to occur
    non_threads = non_threads.extend({"user": non_threads.col("user")})

    joined = kd.record({"threads": threads, "non_threads": non_threads})

    threads = joined.col("threads")
    non_threads = joined.col("non_threads")

    golden.jsonl(
        joined.extend({"joined": threads.else_(non_threads)})
    )
