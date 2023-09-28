import kaskada as kd
import pytest


@pytest.mark.asyncio
async def test_read_csv(golden) -> None:
    content1 = "\n".join(
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
    content2 = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T17:39:57,A,5,10",
            "1996-12-19T17:39:58,B,24,3",
            "1996-12-19T17:39:59,A,17,6",
            "1996-12-19T17:40:00,A,,9",
            "1996-12-19T17:40:01,A,12,",
            "1996-12-19T17:40:02,A,,",
        ]
    )
    source = await kd.sources.CsvString.create(
        content1,
        time_column="time",
        key_column="key",
    )
    golden.jsonl(source)

    await source.add_string(content2)
    golden.jsonl(source)


@pytest.mark.asyncio
async def test_read_csv_with_subsort(golden) -> None:
    content1 = "\n".join(
        [
            "time,key,m,n,subsort",
            "1996-12-19T16:39:57,A,5,10,1",
            "1996-12-19T16:39:58,B,24,3,2",
            "1996-12-19T16:39:59,A,17,6,3",
            "1996-12-19T16:40:00,A,,9,4",
            "1996-12-19T16:40:01,A,12,,5",
            "1996-12-19T16:40:02,A,,,6",
        ]
    )
    content2 = "\n".join(
        [
            "time,key,m,n,subsort",
            "1996-12-19T17:39:57,A,5,10,7",
            "1996-12-19T17:39:58,B,24,3,8",
            "1996-12-19T17:39:59,A,17,6,9",
            "1996-12-19T17:40:00,A,,9,10",
            "1996-12-19T17:40:01,A,12,,11",
            "1996-12-19T17:40:02,A,,,12",
        ]
    )
    source = await kd.sources.CsvString.create(
        content1,
        time_column="time",
        key_column="key",
        subsort_column="subsort",
    )
    golden.jsonl(source)

    await source.add_string(content2)
    golden.jsonl(source)
