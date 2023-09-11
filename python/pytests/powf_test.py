import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57,A,1,2",
            "1996-12-19T16:39:58,B,2,3",
            "1996-12-19T16:39:59,A,3,4",
            "1996-12-19T16:40:00,A,4,5",
            "1996-12-19T16:40:01,A,5,",
            "1996-12-19T16:40:02,A,,6",
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_powf_unwindowed(source, golden) -> None:
    m = source.col("m")
    n = source.col("n")
    golden.jsonl(kd.record({"m": m, "powf": m.powf(n)}))
