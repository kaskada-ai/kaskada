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


async def test_filter(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    condition_m = m > 15
    condition_n = n > 5
    golden.jsonl(
        kd.record(
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


async def test_if_filter_n(source, golden) -> None:
    n = source.col("n")
    condition_n = n > 5
    golden.jsonl(
        kd.record(
            {
                "filter_n": n.if_(condition_n).filter(condition_n),
            }
        )
    )


# Regression test for https://github.com/kaskada-ai/kaskada/issues/766
async def test_filter_to_merge_preserves_interpolation(source, golden) -> None:
    n = source.col("n")
    predicate = n < 9
    golden.jsonl(
        kd.record(
            {"n": n, "predicate": predicate, "filter_sum": n.sum().filter(predicate)}
        )
    )
