import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def source() -> kd.sources.CsvString:
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
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_else_(source, golden) -> None:
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
def record_source() -> kd.sources.JsonlString:
    content = "\n".join(
        [
            """{"time":"1996-12-19T16:39:57","key":"A","override": {"test":"override_val"}}""",
            """{"time":"1996-12-19T16:39:58","key":"A","default_record":{"test":"default"}}""",
        ]
    )
    return kd.sources.JsonlString(content, time_column="time", key_column="key")


def test_else_debug(record_source, golden) -> None:
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
