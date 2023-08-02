"""Tests for the Kaskada query builder."""
import random

import pandas as pd
import pyarrow as pa
import pytest
from sparrow_py.sources import ArrowSource
from sparrow_py.sources import Source


def test_table_valid() -> None:
    """Create a table referencing valid fields."""
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
        ]
    )

    Source("time", "key", schema)


def test_table_invalid_names() -> None:
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
        Source("non_existant_time", "key", schema)

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        Source("time", "non_existant_key", schema)

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        Source(
            "time",
            "key",
            subsort_column_name="non_existant_subsort",
            schema=schema,
        )


def test_add_dataframe() -> None:
    """Test adding a dataframe to a table."""
    member_ids = list(range(0, 10))
    records = []
    for member_id in member_ids:
        for _i in range(0, 100):
            records.append(
                {
                    # number of seconds from epoch
                    "time": random.randint(1000, 9000) * 1000000000000,
                    "key": member_id,
                }
            )

    dataset = pd.DataFrame(records)
    print(dataset.dtypes)
    table = ArrowSource("time", "key", dataset)
    prepared = table.run()
    assert prepared["_time"].is_monotonic_increasing
