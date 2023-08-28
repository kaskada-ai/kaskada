import kaskada as kd
import pytest


@pytest.fixture(scope="module")
def count_if_source() -> kd.sources.CsvString:
    content = "\n".join(
        [
            "time,key,m,n,is_valid",
            "1996-12-19T16:39:57,A,5,10,true",
            "1996-12-19T16:39:58,B,24,3,true",
            "1996-12-19T16:39:59,A,17,6,false",
            "1996-12-19T16:40:00,A,,9,false",
            "1996-12-19T16:40:01,A,12,,true",
            "1996-12-19T16:40:02,A,,,",
            "1996-12-19T16:40:03,B,26,12,true",
            "1996-12-19T16:40:04,B,30,1,true",
        ]
    )
    return kd.sources.CsvString(content, time_column="time", key_column="key")


def test_count_if_unwindowed(count_if_source, golden) -> None:
    is_valid = count_if_source.col("is_valid")
    m = count_if_source.col("m")
    golden.jsonl(
        kd.record(
            {
                "is_valid": is_valid,
                "count_if": is_valid.count_if(),
                "m": m,
            }
        )
    )


def test_count_if_windowed(count_if_source, golden) -> None:
    is_valid = count_if_source.col("is_valid")
    m = count_if_source.col("m")
    golden.jsonl(
        kd.record(
            {
                "is_valid": is_valid,
                "count_if": is_valid.count_if(window=kd.windows.Since(m > 25)),
                "m": m,
            }
        )
    )


def test_count_if_since_true(count_if_source, golden) -> None:
    is_valid = count_if_source.col("is_valid")
    m = count_if_source.col("m")
    golden.jsonl(
        kd.record(
            {
                "is_valid": is_valid,
                "count_if": is_valid.count_if(window=kd.windows.Since(True)),
                "m": m,
            }
        )
    )
