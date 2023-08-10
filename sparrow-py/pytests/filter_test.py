import pytest

import sparrow_py as kt


@pytest.fixture(scope="module")
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


def test_filter(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    condition_m = m > 15
    condition_n = n > 5
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "condition_m": condition_m,
                "filter_m": m.filter(condition_m),
                "n": n,
                "condition_n": condition_n,
                "filter_n": n.filter(condition_n),
            }
        )
    )
