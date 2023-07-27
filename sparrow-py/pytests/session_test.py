"""Tests for the Kaskada session."""
import random

import pandas
import pyarrow as pa
import pytest
from sparrow_py import Session
from sparrow_py.session import Table


table_name = "table1"
time_column = "time"
key_column = "key"
schema = pa.schema(
    [
        pa.field(time_column, pa.int64(), nullable=False),
        pa.field(key_column, pa.int64(), nullable=False),
    ]
)


def generate_dataset(num_members, num_records_per_member):
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
    df = pandas.DataFrame(records)
    return df


def generate_dataset_invalid(num_members, num_records_per_member):
    member_ids = list(range(0, num_members))
    records = []
    for member_id in member_ids:
        for i in range(0, num_records_per_member):
            records.append(
                {
                    "key": member_id,
                    "time": random.randint(
                        1000000000000000, 9000000000000000
                    ),  # number of seconds from epoch
                }
            )
    df = pandas.DataFrame(records)
    return df


@pytest.fixture
def dataset1():
    return generate_dataset(10, 100)


@pytest.fixture
def dataset2():
    return generate_dataset(10, 100)


@pytest.fixture
def mismatched_dataset():
    return generate_dataset_invalid(10, 100)


def test_session_tables() -> None:
    """Test list and add tables to a session."""
    session = Session()
    assert session.tables == []

    table = session.add_table(table_name, time_column, key_column, schema)
    assert session.tables == [table]


def test_table_add_dataframe(dataset1) -> None:
    """Tests adding a dataframe to a table"""
    session = Session()
    table = session.add_table(table_name, time_column, key_column, schema)
    table.add(dataset1)
    assert len(table.data) == 1


def test_create_table_validation() -> None:
    """Tests adding table validation"""
    # Non-existing key column should raise an error
    invalid_key_column = "invalid-key-column"
    with pytest.raises(KeyError):
        Table(table_name, time_column, invalid_key_column, schema, None)

    # Non-existing time column should raise an error
    invalid_time_column = "invalid-time-column"
    with pytest.raises(KeyError):
        Table(table_name, invalid_key_column, key_column, schema, None)

    # Non-existing grouping column should raise an error
    invalid_grouping_column = "invalid-grouping-column"
    with pytest.raises(KeyError):
        Table(
            table_name,
            key_column,
            key_column,
            schema,
            None,
            grouping_name=invalid_grouping_column,
        )

    # Non-existing subsort column should raise an error
    invalid_subsort_column = "invalid-subsort-column"
    with pytest.raises(KeyError):
        Table(
            table_name,
            key_column,
            key_column,
            schema,
            None,
            subsort_column_name=invalid_subsort_column,
        )
