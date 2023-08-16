"""Provide the base-class for Kaskada sources."""
from typing import Optional

import kaskada._ffi as _ffi
import pyarrow as pa

from .._session import _get_session
from .._timestream import Timestream


_TABLE_NUM: int = 0


class Source(Timestream):
    """A source (input) Timestream."""

    # TODO: Clean-up naming on the FFI side.
    _ffi_table: _ffi.Table

    def __init__(
        self,
        schema: pa.Schema,
        time_column_name: str,
        key_column_name: str,
        subsort_column_name: Optional[str] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[str] = None,
    ):
        assert isinstance(schema, pa.Schema)
        """Create a new source."""

        # Fix the schema. The fields should be non-nullable.
        def fix_field(field: pa.Field) -> pa.Field:
            if field.name in [
                time_column_name,
                key_column_name,
                subsort_column_name,
            ]:
                field = field.with_nullable(False)
            if isinstance(field.type, pa.TimestampType):
                field = field.with_type(pa.timestamp(field.type.unit, tz=None))
            return field

        fields = [fix_field(f) for f in schema]
        schema = pa.schema(fields)

        Source._validate_column(time_column_name, schema)
        Source._validate_column(key_column_name, schema)
        Source._validate_column(subsort_column_name, schema)

        # Hack -- Sparrow currently requires tables be named.
        global _TABLE_NUM
        name = f"table{_TABLE_NUM}"
        _TABLE_NUM += 1

        ffi_table = _ffi.Table(
            _get_session(),
            name,
            time_column_name,
            key_column_name,
            schema,
            subsort_column_name,
            grouping_name,
            time_unit,
        )
        super().__init__(ffi_table)
        self._schema = schema
        self._ffi_table = ffi_table

    @property
    def name(self) -> str:
        """Get the current table name."""
        return self._ffi_table.name

    # TODO: Most of these checks exist in Sparrow. We should just surface
    # those errors more cleanly.
    @staticmethod
    def _validate_column(field_name: Optional[str], schema: pa.Schema) -> None:
        if field_name is not None:
            field = schema.field(field_name)
            if field is None:
                raise KeyError(f"Column {field_name!r} does not exist")
            if field.nullable:
                raise ValueError(f"Column: {field_name!r} must be non-nullable")
