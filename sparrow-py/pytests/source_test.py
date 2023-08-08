import random

import pandas as pd
import pyarrow as pa
import pytest
import sparrow_py as kt


def test_table_valid() -> None:
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
        ]
    )

    kt.sources.Source("time", "key", schema)


def test_table_invalid_names() -> None:
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
        kt.sources.Source("non_existant_time", "key", schema)

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        kt.sources.Source("time", "non_existant_key", schema)

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        kt.sources.Source(
            "time",
            "key",
            subsort_column_name="non_existant_subsort",
            schema=schema,
        )


def test_add_dataframe(golden) -> None:
    random.seed(1000)
    member_ids = list(range(0, 10))
    records = []
    for member_id in member_ids:
        for _i in range(0, 10):
            records.append(
                {
                    # number of seconds from epoch
                    "time": random.randint(1000, 4000) * 1000000000000,
                    "key": member_id,
                }
            )
    dataset1 = pd.DataFrame(records)

    table = kt.sources.ArrowSource("time", "key", dataset1)
    golden.jsonl(table)

    records.clear()
    for member_id in member_ids:
        for _i in range(0, 10):
            records.append(
                {
                    # number of seconds from epoch
                    "time": random.randint(3000, 7000) * 1000000000000,
                    "key": member_id,
                }
            )
    dataset2 = pd.DataFrame(records)
    table.add_data(dataset2)
    golden.jsonl(table)
