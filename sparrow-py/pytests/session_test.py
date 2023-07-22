"""Tests for the Kaskada session."""
from sparrow_py import Session


def test_session_tables() -> None:
    """Test list and add tables to a session."""
    session = Session()
    assert session.table_names() == []

    session.add_table("table1")
    assert session.table_names() == ["table1"]


def test_doc() -> None:
    """Test session object documentation."""
    assert Session.__doc__ == "Kaskada session object."
