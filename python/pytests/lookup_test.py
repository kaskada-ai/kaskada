import kaskada as kd
import pytest
<<<<<<< HEAD:python/pytests/lookup_test.py
=======
import sparrow_py as kt
>>>>>>> 571c8f5c (split seconds since previous out):sparrow-py/pytests/lookup_test.py


@pytest.fixture(scope="module")
def key_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,state",
            "1996-12-19T16:39:57,A,WA",
            "1996-12-19T16:39:58,B,NC",
            "1996-12-19T16:39:59,A,WA",
            "1996-12-19T16:40:00,A,NC",
            "1996-12-19T16:40:01,A,SC",
            "1996-12-19T16:40:02,A,WA",
        ]
    )
    return kd.sources.CsvString(content, time_column_name="time", key_column_name="key")


@pytest.fixture(scope="module")
def foreign_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,NC,10,5",
            "1996-12-19T16:39:58,NC,24,3",
            "1996-12-19T16:39:59,WA,17,6",
            "1996-12-19T16:40:00,NC,,9",
            "1996-12-19T16:40:01,WA,12,",
            "1996-12-19T16:40:02,WA,,",
        ]
    )
    return kd.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_lookup(key_source, foreign_source, golden) -> None:
    state = key_source.col("state")
    foreign_value = foreign_source.col("m")
    last_foreign_value = foreign_source.col("m").last()
    golden.jsonl(
        kd.record(
            {
                "state": state,
                "lookup": foreign_value.lookup(state),
                "lookup_last": last_foreign_value.lookup(state),
            }
        )
    )
