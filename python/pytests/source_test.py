import kaskada as kd
import pyarrow as pa
import pytest


def test_table_valid() -> None:
    schema = pa.schema(
        [
            pa.field("time", pa.int32(), nullable=False),
            pa.field("key", pa.int64(), nullable=False),
        ]
    )

    kd.sources.Source(schema, time_column="time", key_column="key")


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
        kd.sources.Source(schema, time_column="non_existant_time", key_column="key")

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        kd.sources.Source(schema, time_column="time", key_column="non_existant_key")

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        kd.sources.Source(
            schema,
            time_column="time",
            key_column="key",
            subsort_column="non_existant_subsort",
        )
