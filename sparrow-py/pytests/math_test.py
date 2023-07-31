"""Tests for the Kaskada query builder."""
import random
import sys

import pandas as pd
import pyarrow as pa
import pytest
from sparrow_py import Session
from sparrow_py import Table
from sparrow_py import math
from io import StringIO


@pytest.fixture
def session() -> Session:
    """Create a session for testing."""
    session = Session()
    return session

@pytest.fixture
def table_int64(session: Session) -> Table:
    """Create an empty table for testing."""
    schema = pa.schema(
        [
            pa.field("time", pa.string(), nullable=False),
            pa.field("key", pa.string(), nullable=False),
            pa.field("m", pa.int64()),
            pa.field("n", pa.int64()),
        ]
    )
    return Table(session, "table1", "time", "key", schema)

def read_csv(csv_string: str, **kwargs) -> pd.DataFrame:
    """Read CSV from a string."""
    return pd.read_csv(StringIO(csv_string), dtype_backend = 'pyarrow', **kwargs)

def test_read_table(session, table_int64) -> None:
    input = read_csv('\n'.join([
        "time,subsort,key,m,n",
        "1996-12-19T16:39:57-08:00,0,A,5,10",
        "1996-12-19T16:39:58-08:00,0,B,24,3",
        "1996-12-19T16:39:59-08:00,0,A,17,6",
        "1996-12-19T16:40:00-08:00,0,A,,9",
        "1996-12-19T16:40:01-08:00,0,A,12,",
        "1996-12-19T16:40:02-08:00,0,A,,"
    ]), dtype = {
        'm': 'Int64',
        'n': 'Int64',
    })
    table_int64.add_data(input)

    # TODO: Options for outputting to different destinations (eg., to CSV).
    # TODO: Allow running a single expression (eg., without field names)

    #result = (table_int64.m + tableint64.n).execute()
    result = table_int64.execute()
    pd.testing.assert_series_equal(input['time'], result['time'], check_dtype=False)
    pd.testing.assert_series_equal(input['m'], result['m'], check_dtype=False)
    pd.testing.assert_series_equal(input['n'], result['n'], check_dtype=False)