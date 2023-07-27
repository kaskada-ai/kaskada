from typing import Optional
from typing import List

import pyarrow as pa
from sparrow_py import Expr
from sparrow_py import _ffi


class Table(Expr):
    """ "A table expression."""

    def __init__(self, name: str, ffi: _ffi.Expr):
        """Create a new table expression."""
        self.name = name
        self._ffi = ffi


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
        table = Table(name, ffi)
        self.tables.append(table)
        return table
