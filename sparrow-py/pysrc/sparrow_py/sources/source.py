"""Provide the base-class for Kaskada sources."""
from typing import Optional

import pyarrow as pa
import sparrow_py as kt
import sparrow_py._ffi as _ffi

from .._session import _get_session


_TABLE_NUM: int = 0


class Source(kt.Expr):
    """A source expression."""

    # TODO: Clean-up naming on the FFI side.
    _ffi_table: _ffi.Table

    def __init__(
        self,
        time_column_name: str,
        key_column_name: str,
        schema: pa.Schema,
        subsort_column_name: Optional[str] = None,
        grouping_name: Optional[str] = None,
    ):
        """Create a new source."""
        Source._validate_column(time_column_name, schema)
        Source._validate_column(key_column_name, schema)
        Source._validate_column(subsort_column_name, schema)

        # Hack -- Sparrow currently requires tables be named.
        global _TABLE_NUM
        name = f"table{_TABLE_NUM}"
        _TABLE_NUM += 1

        self._ffi_table = _ffi.Table(
            _get_session(),
            name,
            time_column_name,
            key_column_name,
            schema,
            subsort_column_name,
            grouping_name,
        )
        super().__init__(self._ffi_table)

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
