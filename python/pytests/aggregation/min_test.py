import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
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
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_min_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "min_m": m.min(), "n": n, "min_n": n.min()}))


async def test_min_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "since_min_m": m.min(window=kd.windows.Since(m > 20)),
                "n": n,
                "sliding_min_n": n.min(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


async def test_min_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals the original vaule.
    m_min_since_true = kd.record(
        {
            "m": source.col("m"),
            "m_min": source.col("m").min(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_min_since_true)
