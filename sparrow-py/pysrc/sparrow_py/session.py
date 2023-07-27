from typing import List
from typing import Optional

import pandas as pd
import pyarrow as pa
from sparrow_py import Expr
from sparrow_py import _ffi


class Table(Expr):
    """ "A table expression."""

    data: List[pa.Table] = []

    @classmethod
    def validate_table(
        name: str,
        time_column_name: str,
        key_column_name: str,
        schema: pa.Schema,
        subsort_column_name: Optional[str] = None,
        grouping_name: Optional[str] = None,
    ) -> None:
        # Checking if the column exist. If they do not, it'll throw a KeyError.
        key_field = schema.field(key_column_name)
        if key_field.nullable:
            raise KeyError(f"Key Column: {key_column_name} must be non-nullable")
        time_field = schema.field(time_column_name)
        if time_field.nullable:
            raise KeyError(f"Time Column: {time_column_name} must be non-nullable")

        if subsort_column_name is not None:
            subsort_field = schema.field(subsort_column_name)
            if subsort_field.nullable:
                raise KeyError(
                    f"Subsort Column: {subsort_column_name} must be non-nullable"
                )
        if grouping_name is not None:
            grouping_field = schema.field(grouping_name)
            if grouping_field.nullable:
                raise KeyError(f"Grouping Column: {grouping_name} must be non-nullable")

    def __init__(
        self,
        name: str,
        time_column_name: str,
        key_column_name: str,
        schema: pa.Schema,
        ffi: _ffi.Expr,
        subsort_column_name: Optional[str] = None,
        grouping_name: Optional[str] = None,
    ):
        """Create a new table expression."""
        Table.validate_table(
            time_column_name,
            key_column_name,
            schema,
            subsort_column_name=subsort_column_name,
            grouping_name=grouping_name,
        )
        self.name = name
        self.time_column_name = time_column_name
        self.key_column_name = key_column_name
        self.schema = schema
        self.subsort_column_name = subsort_column_name
        self.grouping_name = grouping_name
        self._ffi = ffi

    def add(self, df: pd.DataFrame) -> None:
        """Add a dataframe to a table"""
        pa_table = pa.Table.from_pandas(df)
        self.data.append(pa_table)


class Session(object):
    """A Kaskada session."""

    tables: List[Table]

    def __init__(self) -> None:
        """Create a new session."""
        self._session = _ffi.Session()
        self.tables = []

    def add_table(
        self,
        name: str,
        time_column_name: str,
        key_column_name: str,
        schema: pa.Schema,
        subsort_column_name: Optional[str] = None,
        grouping_name: Optional[str] = None,
    ) -> Table:
        ffi = self._session.add_table(
            name,
            time_column_name,
            key_column_name,
            schema,
            subsort_column_name,
            grouping_name,
        )
        table = Table(
            name,
            time_column_name,
            key_column_name,
            schema,
            ffi,
            subsort_column_name=subsort_column_name,
            grouping_name=grouping_name,
        )
        self.tables.append(table)
        return table
