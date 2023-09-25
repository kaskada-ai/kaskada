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


async def test_count_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record({"m": m, "count_m": m.count(), "n": n, "count_n": n.count()})
    )


async def test_count_windowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "since_count_m": m.count(window=kd.windows.Since(m > 20)),
                "n": n,
                "sliding_count_n": n.count(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


async def test_count_sliding(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "count_n": n.count(window=kd.windows.Sliding(2, m > 10)),
            }
        )
    )


async def test_count_since_true(source, golden) -> None:
    # `since(True)` should be the same as unwindowed, so equals to one whenever the value is non-null
    m_sum_since_true = kd.record(
        {
            "m": source.col("m"),
            "since_m": source.col("m").count(window=kd.windows.Since(True)),
        }
    )
    golden.jsonl(m_sum_since_true)
