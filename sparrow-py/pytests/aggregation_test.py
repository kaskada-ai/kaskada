"""Tests for the Kaskada query builder."""
import pytest
from sparrow_py import SinceWindow
from sparrow_py import SlidingWindow
from sparrow_py import record
from sparrow_py.sources import CsvSource


@pytest.fixture(scope="module")
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

def test_sum_unwindowed(source, golden) -> None:
    """Test we can create a record."""
    m = source["m"]
    n = source["n"]
    golden(record({"m": m, "sum_m": m.sum(), "n": n, "sum_n": n.sum()}))


def test_sum_windowed(source, golden) -> None:
    """Test we can create a record."""
    m = source["m"]
    n = source["n"]
    golden(
        record(
            {
                "m": m,
                "sum_m": m.sum(window=SinceWindow(m > 20)),
                "n": n,
                "sum_n": n.sum(window=SlidingWindow(2, m > 10)),
            }
        )
    )
