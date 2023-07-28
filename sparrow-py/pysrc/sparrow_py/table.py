from typing import Optional

import pandas as pd
import pyarrow as pa
import sparrow_py._ffi as _ffi
from sparrow_py.expr import Expr
from sparrow_py.session import Session


class Table(Expr):
    """ "A table expression."""

    _ffi_table: _ffi.Table

    def __init__(
        self,
        session: Session,
        name: str,
        time_column_name: str,
        key_column_name: str,
        schema: pa.Schema,
        subsort_column_name: Optional[str] = None,
        grouping_name: Optional[str] = None,
    ):
        """Create a new table expression."""
        Table._validate_column(time_column_name, schema)
        Table._validate_column(key_column_name, schema)
        Table._validate_column(subsort_column_name, schema)
        self._ffi_table = _ffi.Table(
            session._ffi,
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
        """Get the current voltage."""
        return self._ffi_table.name

    def add_data(self, df: pd.DataFrame) -> None:
        """Add a dataframe to a table."""
        pa_table = pa.RecordBatch.from_pandas(df)
        self._ffi_table.add_pyarrow(pa_table)

    # TODO: Most of these checks exist in Sparrow. We should just surface
    # those errors more cleanly.
    @staticmethod
    def _validate_column(field_name: Optional[str], schema: pa.Schema) -> None:
        if field_name is not None:
            field = schema.field(field_name)
            if field is None:
                raise KeyError(f"Column '{field_name}' does not exist")
            if field.nullable:
                raise ValueError(f"Column: '{field_name}' must be non-nullable")
