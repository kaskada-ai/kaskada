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

    kt.sources.Source(schema, time_column_name="time", key_column_name="key")


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
        kt.sources.Source(schema, time_column_name="non_existant_time", key_column_name="key")

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        kt.sources.Source(schema, time_column_name="time", key_column_name="non_existant_key")

    with pytest.raises(KeyError):
        # Currently, this doesn't propagate the suggestions from
        # existing column names from Sparrow.
        # TODO: Do that.
        kt.sources.Source(
            schema,
            time_column_name="time",
            key_column_name="key",
            subsort_column_name="non_existant_subsort",
        )