"""Tests for the Kaskada query builder."""
import random
import sys

import pandas as pd
import pyarrow as pa
import pytest
from sparrow_py import Session
from sparrow_py import Table
from sparrow_py import math


@pytest.fixture
def session() -> Session:
    """Create a session for testing."""
    session = Session()
    return session


@pytest.fixture
def dataset(
    num_members: int = 10, num_records_per_member: int = 100000
) -> pd.DataFrame:
    member_ids = list(range(0, num_members))
    records = []
    for member_id in member_ids:
        for i in range(0, num_records_per_member):
            records.append(
                {
                    "time": random.randint(
                        1000000000000000, 9000000000000000
                    ),  # number of seconds from epoch
                    "key": member_id,
                }
            )
    df = pd.DataFrame(records)
    return df


def test_table_valid(session) -> None:
    """Create a table referencing valid fields."""
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
        ]
    )

    table = Table(session, "table1", "time", "key", schema)
    assert table.name == "table1"


def test_table_invalid_names(session) -> None:
    """Create a table with invalid column names."""
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
        ]
    )

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        Table(session, "table1", "non_existant_time", "key", schema)

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        Table(session, "table1", "time", "non_existant_key", schema)

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        Table(
            session,
            "table1",
            "time",
            "key",
            subsort_column_name="non_existant_subsort",
            schema=schema,
        )


def test_add_dataframe(session, dataset) -> None:
    """Test adding a dataframe to a table."""
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
        ]
    )
    table = Table(session, "table1", "time", "key", schema)
    assert table._ffi_table.num_data == 0
    table.add_data(dataset)
    assert table._ffi_table.num_data == 1
