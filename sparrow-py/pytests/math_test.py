"""Tests for the Kaskada query builder."""
import pytest
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


def test_read_table(source_int64) -> None:
    """Test we can read a table and do basic math."""
    # TODO: Options for outputting to different destinations (eg., to CSV).
    # TODO: Allow running a single expression (eg., without field names)

    # result = (source_int64.m + source_int64.n).run()
    result = source_int64.run_to_csv_string()
    print(result)

    assert result == "\n".join(
        [
            "_time,_subsort,_key_hash,_key,time,key,m,n",
            "1996-12-20 00:39:57,0,12960666915911099378,A,1996-12-19T16:39:57-08:00,A,5.0,10.0",
            "1996-12-20 00:39:58,1,2867199309159137213,B,1996-12-19T16:39:58-08:00,B,24.0,3.0",
            "1996-12-20 00:39:59,2,12960666915911099378,A,1996-12-19T16:39:59-08:00,A,17.0,6.0",
            "1996-12-20 00:40:00,3,12960666915911099378,A,1996-12-19T16:40:00-08:00,A,,9.0",
            "1996-12-20 00:40:01,4,12960666915911099378,A,1996-12-19T16:40:01-08:00,A,12.0,",
            "1996-12-20 00:40:02,5,12960666915911099378,A,1996-12-19T16:40:02-08:00,A,,",
            "",
        ]
    )
