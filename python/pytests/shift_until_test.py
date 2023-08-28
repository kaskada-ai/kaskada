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
            "1996-12-19T16:40:00,A,10,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_shift_until_predicate(source, golden) -> None:
    m = source.col("m")
    predicate = m.sum() > 30
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "sum_m": m.sum(),
                "predicate": predicate,
                "shift_until": m.last().shift_until(predicate),
            }
        )
    )
