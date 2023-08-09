import pytest
import sparrow_py as kt


@pytest.fixture(scope="module")
def source() -> kt.sources.CsvSource:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,A,5,10",
            "1996-12-19T16:39:58-08:00,B,24,3",
            "1996-12-19T16:39:59-08:00,A,17,6",
            "1996-12-19T16:40:00-08:00,A,,9",
            "1996-12-19T16:40:01-08:00,A,12,",
            "1996-12-19T16:40:02-08:00,A,,",
        ]
    )
    return kt.sources.CsvSource("time", "key", content)


def test_sum_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kt.record({"m": m, "sum_m": m.sum(), "n": n, "sum_n": n.sum()}))


def test_sum_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kt.record(
            {
                "m": m,
                "sum_m": m.sum(window=kt.windows.Since(m > 20)),
                "n": n,
                "sum_n": n.sum(window=kt.windows.Sliding(2, m > 10)),
            }
        )
    )
