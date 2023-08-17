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
            "1996-12-19T16:40:00,A,,9",
            "1996-12-19T16:40:01,A,12,",
            "1996-12-19T16:40:02,A,,",
        ]
    )
    return kd.sources.CsvString(content, time_column_name="time", key_column_name="key")


def test_max_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "max_m": m.max(), "n": n, "max_n": n.max()}))


def test_max_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "max_m": m.max(window=kd.windows.Since(m > 20)),
                "n": n,
                "max_n": n.max(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


def test_max_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals the original vaule.
    m_max_since_true = kd.record(
        {
            "m": source.col("m"),
            "m_max": source.col("m").max(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_max_since_true)