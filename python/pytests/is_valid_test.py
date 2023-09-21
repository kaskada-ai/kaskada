import kaskada as kd
import pytest

@pytest.fixture(scope="module")
async def boolean_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,a,b",
            '1996-12-19T16:39:57,A,true,true',
            '1996-12-19T16:39:58,B,false,false',
            '1996-12-19T16:39:59,B,,true',
            '1996-12-19T16:40:00,B,true,false',
            '1996-12-19T16:40:01,B,false,true',
            '1996-12-19T16:40:02,B,false,',
            '1996-12-19T16:40:02,B,,',
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )

@pytest.fixture(scope="module")
async def f64_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            '1996-12-19T16:39:57,A,5.2,10',
            '1996-12-19T16:39:58,B,24.3,3.9',
            '1996-12-19T16:39:59,A,17.6,6.2',
            '1996-12-19T16:40:00,A,,9.25',
            '1996-12-19T16:40:01,A,12.4,',
            '1996-12-19T16:40:02,A,,',
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )

@pytest.fixture(scope="module")
async def i64_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n",
            '1996-12-19T16:39:57,A,5,10',
            '1996-12-19T16:39:58,B,24,3',
            '1996-12-19T16:39:59,A,17,6',
            '1996-12-19T16:40:00,A,,9',
            '1996-12-19T16:40:01,A,12,',
            '1996-12-19T16:40:02,A,,',
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )

@pytest.fixture(scope="module")
async def string_source() -> kd.sources.CsvString:
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

@pytest.fixture(scope="module")
async def timestamp_ns_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,n,m,other_time,fruit",
            '1996-12-19T16:39:57,A,2,4,2003-12-19T16:39:57,pear',
            '1996-12-19T16:39:58,B,4,3,1994-11-19T16:39:57,watermelon',
            '1996-12-19T16:39:59,B,5,,1998-12-19T16:39:57,mango',
            '1996-12-19T16:40:00,B,,,1992-12-19T16:39:57,',
            '1996-12-19T16:40:01,B,8,8,,',
            '1996-12-19T16:40:02,B,23,11,1994-12-19T16:39:57,mango',
        ]
    )
    return await kd.sources.CsvString.create(
        content, time_column="time", key_column="key"
    )

async def test_is_valid_boolean(boolean_source, golden) -> None:
    a = boolean_source.col("a")
    golden.jsonl(
        kd.record(
            {
                "a": a,
                "is_valid": a.is_valid(),
            }
        )
    )

async def test_is_valid_f64(f64_source, golden) -> None:
    m = f64_source.col("m")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "is_valid": m.is_valid(),
            }
        )
    )

async def test_is_valid_i64(i64_source, golden) -> None:
    m = i64_source.col("m")
    golden.jsonl(
        kd.record(
            {
                "m": m,
                "is_valid": m.is_valid(),
            }
        )
    )

async def test_is_valid_string(string_source, golden) -> None:
    s = string_source.col("s")
    golden.jsonl(
        kd.record(
            {
                "s": s,
                "is_valid": s.is_valid(),
            }
        )
    )

async def test_is_valid_timestamp_ns(timestamp_ns_source, golden) -> None:
    n = timestamp_ns_source.col("n")
    golden.jsonl(
        kd.record(
            {
                "n": n,
                "is_valid": n.is_valid(),
            }
        )
    )

async def test_is_valid_record(timestamp_ns_source, golden) -> None:
    golden.jsonl(
        kd.record(
            {
                "is_valid": timestamp_ns_source.is_valid()
            }
        )
    )
