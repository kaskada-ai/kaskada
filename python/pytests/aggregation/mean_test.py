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
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_mean_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "mean_m": m.mean(), "n": n, "mean_n": n.mean()}))


def test_mean_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "mean_m": m.mean(window=kd.windows.Since(m > 20)),
                "n": n,
                "mean_n": n.mean(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


def test_mean_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals the original vaule.
    m_mean_since_true = kd.record(
        {
            "m": source.col("m"),
            "m_mean": source.col("m").mean(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_mean_since_true)
