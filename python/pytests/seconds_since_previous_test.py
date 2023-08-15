import pytest
import kaskada as kd

@pytest.fixture
def source() -> kt.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n,t",
            "1996-12-19T16:39:57,A,5,10,1996-12-19T16:42:57",
            "1996-12-19T16:39:58,B,24,3,1996-12-19T16:39:59",
            "1996-12-19T16:39:59,A,17,6,",
            "1996-12-19T16:40:00,A,,9,1996-12-19T16:41:00",
            "1996-12-19T16:40:01,A,12,,1996-12-19T16:42:01",
            "1996-12-19T16:40:02,A,,,1996-12-19T16:43:02",
        ]
    )
    return kt.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_seconds_since_previous(golden, source) -> None:
    t = source.col("time")
    golden.jsonl(
        kd.record(
            {
                "seconds_since": t.seconds_since_previous(),
                "seconds_since_1": t.seconds_since_previous(1),
                "seconds_since_2": t.seconds_since_previous(2),
            }
        )
    )
