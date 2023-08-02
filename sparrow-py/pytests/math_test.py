"""Tests for the Kaskada query builder."""
import pytest
import sparrow_py as s
from sparrow_py.sources import CsvSource


@pytest.fixture
def source_int64() -> CsvSource:
    """Create an empty table for testing."""
    content = "\n".join(
        [
            "time,key,m,n",
            "1996-12-19T16:39:57-08:00,A,5,10",
            "1996-12-19T16:39:58-08:00,B,24,3",
            "1996-12-19T16:39:59-08:00,A,17,6",
            "1996-12-19T16:40:00-08:00,A,,9",
            "1996-12-19T16:40:01-08:00,A,12,",
            "1996-12-19T16:40:02-08:00,A,,",
        ]
    )
    return CsvSource("time", "key", content)


def test_math_int64(source_int64) -> None:
    """Test we can read a table and do basic math."""
    m = source_int64["m"]
    n = source_int64["n"]
    result = s.record(
        {
            "m": m,
            "n": n,
            "add": m + n,
            "sub": m - n,
        }
    ).run_to_csv_string()

    assert result == "\n".join(
        [
            "_time,_subsort,_key_hash,_key,m,n,add,sub",
            "1996-12-20 00:39:57,0,12960666915911099378,A,5.0,10.0,15.0,-5.0",
            "1996-12-20 00:39:58,1,2867199309159137213,B,24.0,3.0,27.0,21.0",
            "1996-12-20 00:39:59,2,12960666915911099378,A,17.0,6.0,23.0,11.0",
            "1996-12-20 00:40:00,3,12960666915911099378,A,,9.0,,",
            "1996-12-20 00:40:01,4,12960666915911099378,A,12.0,,,",
            "1996-12-20 00:40:02,5,12960666915911099378,A,,,,",
            "",
        ]
    )
