"""Provide sources based on PyArrow, including Pandas and CSV."""
from io import StringIO
from typing import Optional
from typing import Union

import pandas as pd
import pyarrow as pa

from .source import Source


class ArrowSource(Source):
    """A source reading data batches from Arrow or Pandas."""

    def __init__(
        self,
        time_column_name: str,
        key_column_name: str,
        data: Union[pd.DataFrame, pa.RecordBatch],
        subsort_column_name: Optional[str] = None,
        grouping_name: Optional[str] = None,
    ):
        """Create a new source reading from Arrow or Pandas."""
        if isinstance(data, pd.DataFrame):
            schema = pa.Schema.from_pandas(data)

            def fix_nullable(field: pa.Field) -> pa.Field:
                if field.name in [
                    time_column_name,
                    key_column_name,
                    subsort_column_name,
                ]:
                    return field.with_nullable(False)
                else:
                    return field

            fields = [fix_nullable(f) for f in schema]
            schema = pa.schema(fields)
            data = pa.RecordBatch.from_pandas(data, schema)

        super().__init__(
            time_column_name,
            key_column_name,
            data.schema,
            subsort_column_name,
            grouping_name,
        )

        self._ffi_table.add_pyarrow(data)


class CsvSource(ArrowSource):
    """Source reading data from CSV via Pandas."""

    def __init__(
        self, time_column_name: str, key_column_name: str, csv_string: str, **kwargs
    ) -> None:
        content = pd.read_csv(StringIO(csv_string), dtype_backend="pyarrow", **kwargs)
        super().__init__(time_column_name, key_column_name, content)
