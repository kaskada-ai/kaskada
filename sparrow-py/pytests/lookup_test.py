import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def key_source() -> kt.sources.CsvSource:
    content = "\n".join(
        [
            "time,key,state",
            "1996-12-19T16:39:57-08:00,A,WA",
            "1996-12-19T16:39:58-08:00,B,NC",
            "1996-12-19T16:39:59-08:00,A,WA",
            "1996-12-19T16:40:00-08:00,A,NC",
            "1996-12-19T16:40:01-08:00,A,SC",
            "1996-12-19T16:40:02-08:00,A,WA",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)


@pytest.fixture(scope="module")
def foreign_source() -> kt.sources.CsvSource:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,NC,10,5",
            "1996-12-19T16:39:58-08:00,NC,24,3",
            "1996-12-19T16:39:59-08:00,WA,17,6",
            "1996-12-19T16:40:00-08:00,NC,,9",
            "1996-12-19T16:40:01-08:00,WA,12,",
            "1996-12-19T16:40:02-08:00,WA,,",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)


def test_lookup(key_source, foreign_source, golden) -> None:
    state = key_source.col("state")
    foreign_value = foreign_source.col("m")
    last_foreign_value = foreign_source.col("m").last()
    golden.jsonl(
        kt.record(
            {
                "state": state,
                "lookup": foreign_value.lookup(state),
                "lookup_last": last_foreign_value.lookup(state),
            }
        )
    )
