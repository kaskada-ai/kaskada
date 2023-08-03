"""Tests for the collect function."""
import pytest
from sparrow_py.sources import CsvSource
from sparrow_py import record
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


def test_collect_basic(source, golden) -> None:
    """Test we can collect values to a list"""
    m = source["m"]
    n = source["n"]
    golden(record(
        {
            "m": m,
            "collect_m": m.collect(),
            "n": n,
            "collect_n": n.collect(),
        }
    ))

def test_collect_with_max(source, golden) -> None:
    """Test we can collect values to a list"""
    m = source["m"]
    n = source["n"]
    golden(record(
        {
            "m": m,
            "collect_m_max_2": m.collect(max = 2),
            "n": n,
            "collect_n_max_2": n.collect(max = 2),
        }
    ))






