import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source() -> kt.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,A,5,10",
            "1996-12-19T16:39:58-08:00,B,24,3",
            "1996-12-19T16:39:59-08:00,A,17,6",
            "1996-12-19T16:40:00-08:00,A,10,9",
            "1996-12-19T16:40:01-08:00,A,12,",
            "1996-12-19T16:40:02-08:00,A,,",
        ]
    )
    return kt.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_shift_until_predicate(source, golden) -> None:
    m = source.col("m")
    predicate = m.sum() > 30
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "sum_m": m.sum(),
                "predicate": predicate,
                "shift_until": m.last().shift_until(predicate),
            }
        )
    )
