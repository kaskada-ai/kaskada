import kaskada as kd
import pytest


@pytest.fixture(scope="module")
async def source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,s,n,t",
            '1996-12-19T16:39:57,A,"hEllo",0,"hEllo"',
            '1996-12-19T16:39:58,B,"World",5,"world"',
            '1996-12-19T16:39:59,B,"hello world",-2,"hello world"',
            '1996-12-19T16:40:00,B,,-2,"greetings"',
            '1996-12-19T16:40:01,B,,2,"salutations"',
            '1996-12-19T16:40:02,B,"goodbye",,',
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )


async def test_substring(source, golden) -> None:
    s = source.col("s")
    n = source.col("n")
    golden.jsonl(
        kd.record(
            {
                "substring_0_2": s.substring(start=0, end=2),
                "substring_1": s.substring(start=1),
                "substring_0_i": s.substring(end=n),
                "substring_i": s.substring(start=n),
            }
        )
    )
