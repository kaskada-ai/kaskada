"""Tests for the collect function."""
import pytest
import sparrow_py as s
from sparrow_py.sources import CsvSource
from sparrow_py import SlidingWindow, SinceWindow

@pytest.fixture(scope = "module")
def source() -> CsvSource:
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


def test_move_me(source) -> None:
    """Test we can collect values to a list"""
    m = source["m"]
    n = source["n"]
    result = s.record(
        {
            "m": m,
            "collect_m": m.collect(),
            "collect_m_max_2": m.collect(max = 2),
            "n": n,
            "collect_n": n.collect(),
            "collect_n_max_2": n.collect(max = 2),
        }
    ).run_to_csv_string()

    assert result == "\n".join(
        [
            "_time,_subsort,_key_hash,_key,m,sum_m,n,sum_n",
            "1996-12-20 00:39:57,0,12960666915911099378,A,5.0,5,10.0,10",
            "1996-12-20 00:39:58,1,2867199309159137213,B,24.0,24,3.0,3",
            "1996-12-20 00:39:59,2,12960666915911099378,A,17.0,22,6.0,16",
            "1996-12-20 00:40:00,3,12960666915911099378,A,,22,9.0,25",
            "1996-12-20 00:40:01,4,12960666915911099378,A,12.0,34,,25",
            "1996-12-20 00:40:02,5,12960666915911099378,A,,34,,25",
            "",
        ]
    )


