"""Tests for the Kaskada session."""
import pyarrow as pa
from sparrow_py import Session


def test_session_tables() -> None:
    """Test list and add tables to a session."""
    session = Session()
    assert session.tables == []

    schema = pa.schema([("time", pa.int32()), ("key", pa.int64())])
    table = session.add_table("table1", "time", "key", schema)
    assert session.tables == [table]
