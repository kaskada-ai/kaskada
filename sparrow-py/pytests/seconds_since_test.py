import pytest
import sparrow_py as kt


@pytest.fixture
def source() -> kt.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,5,10",
            "1996-12-19T16:39:58,B,24,3",
            "1996-12-19T16:39:59,A,17,6",
            "1996-12-19T16:40:00,A,,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    return kt.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_seconds_since(golden, source) -> None:
    t = source.col("time")
    golden.jsonl(
        kt.record(
            {
                "time": t,
                "seconds_since_previous": t.seconds_since_previous(),
                "seconds_since_previous_2": t.seconds_since_previous(2),
            }
        )
    )
