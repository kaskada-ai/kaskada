"""Provide sources based on PyArrow, including Pandas and CSV."""
from __future__ import annotations

from io import BytesIO
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.csv
import pyarrow.json
import pyarrow.parquet

from .source import Source, TimeUnit


class Pandas(Source):
    """Source reading data from Pandas dataframe."""

    def __init__(
        self,
        dataframe: pd.DataFrame,
        *,
        time_column: str,
        key_column: str,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a source reading Pandas DataFrames.

        Args:
            dataframe: The DataFrame to start from.
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            subsort_column: The name of the column containing the subsort.
              If not provided, the subsort will be assigned by the system.
            schema: The schema to use. If not provided, it will be inferred from the input.
            grouping_name: The name of the group associated with each key.
              This is used to ensure implicit joins are only performed between data grouped
              by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
              If not specified (and not specified in the data), nanosecond will be assumed.
        """
        if schema is None:
            schema = pa.Schema.from_pandas(dataframe)
        super().__init__(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )
        self.add_data(dataframe)

    def add_data(self, data: pd.DataFrame) -> None:
        """Add data to the source."""
        table = pa.Table.from_pandas(data, self._schema, preserve_index=False)
        for batch in table.to_batches():
            self._ffi_table.add_pyarrow(batch)


class PyDict(Source):
    """Source reading data from lists of dicts."""

    def __init__(
        self,
        rows: dict | list[dict],
        *,
        time_column: str,
        key_column: str,
        retained: bool = True,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a source reading from rows represented as dicts.

        Args:
            rows: One or more rows represented as dicts.
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            retained: Whether added rows should be retained for queries.
              If True, rows (both provided to the constructor and added later) will be retained
              for interactive queries. If False, rows will be discarded after being sent to any
              running materializations. Consider setting this to False when the source will only
              be used for materialization to avoid unnecessary memory usage.
            subsort_column: The name of the column containing the subsort.
              If not provided, the subsort will be assigned by the system.
            schema: The schema to use. If not provided, it will be inferred from the input.
            grouping_name: The name of the group associated with each key.
              This is used to ensure implicit joins are only performed between data grouped
              by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
              If not specified (and not specified in the data), nanosecond will be assumed.
        """
        if schema is None:
            schema = pa.Table.from_pylist(rows).schema
        super().__init__(
            retained=retained,
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        self._convert_options = pyarrow.csv.ConvertOptions(column_types=schema)
        self.add_rows(rows)

    def add_rows(self, rows: dict | list[dict]) -> None:
        """Add data to the source."""
        if isinstance(rows, dict):
            rows = [rows]
        table = pa.Table.from_pylist(rows, schema=self._schema)
        for batch in table.to_batches():
            self._ffi_table.add_pyarrow(batch)


# TODO: We should be able to go straight from CSV to PyArrow, but
# currently that has some problems with timestamp hadling.
class CsvString(Source):
    """Source reading data from CSV strings using Pandas."""

    def __init__(
        self,
        csv_string: str | BytesIO,
        *,
        time_column: str,
        key_column: str,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a CSV String Source.

        Args:
            csv_string: The CSV string to start from.
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            subsort_column: The name of the column containing the subsort.
              If not provided, the subsort will be assigned by the system.
            schema: The schema to use. If not provided, it will be inferred from the input.
            grouping_name: The name of the group associated with each key.
              This is used to ensure implicit joins are only performed between data grouped
              by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
              If not specified (and not specified in the data), nanosecond will be assumed.
        """
        if isinstance(csv_string, str):
            csv_string = BytesIO(csv_string.encode("utf-8"))
        if schema is None:
            schema = pa.csv.read_csv(csv_string).schema
            csv_string.seek(0)
        super().__init__(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        self._convert_options = pyarrow.csv.ConvertOptions(
            column_types=schema,
            strings_can_be_null=True,
        )
        self.add_string(csv_string)

    def add_string(self, csv_string: str | BytesIO) -> None:
        """Add data to the source."""
        if isinstance(csv_string, str):
            csv_string = BytesIO(csv_string.encode("utf-8"))
        content = pa.csv.read_csv(csv_string, convert_options=self._convert_options)
        for batch in content.to_batches():
            self._ffi_table.add_pyarrow(batch)


class JsonlString(Source):
    """Source reading data from line-delimited JSON strings using PyArrow."""

    def __init__(
        self,
        json_string: str | BytesIO,
        *,
        time_column: str,
        key_column: str,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a JSON String Source.

        Args:
            json_string: The line-delimited JSON string to start from.
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            subsort_column: The name of the column containing the subsort.
              If not provided, the subsort will be assigned by the system.
            schema: The schema to use. If not provided, it will be inferred from the input.
            grouping_name: The name of the group associated with each key.
              This is used to ensure implicit joins are only performed between data grouped
              by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
              If not specified (and not specified in the data), nanosecond will be assumed.
        """
        if isinstance(json_string, str):
            json_string = BytesIO(json_string.encode("utf-8"))
        if schema is None:
            schema = pa.json.read_json(json_string).schema
            json_string.seek(0)
        super().__init__(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        self._parse_options = pyarrow.json.ParseOptions(explicit_schema=schema)
        self.add_string(json_string)

    def add_string(self, json_string: str | BytesIO) -> None:
        """Add data to the source."""
        if isinstance(json_string, str):
            json_string = BytesIO(json_string.encode("utf-8"))
        batches = pa.json.read_json(json_string, parse_options=self._parse_options)
        for batch in batches.to_batches():
            self._ffi_table.add_pyarrow(batch)


class Parquet(Source):
    """Source reading data from Parquet files."""

    def __init__(
        self,
        path: str,
        *,
        time_column: str,
        key_column: str,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a Parquet source.

        Args:
            path: The path to the Parquet file to add.
            dataframe: The DataFrame to start from.
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            subsort_column: The name of the column containing the subsort.
              If not provided, the subsort will be assigned by the system.
            schema: The schema to use. If not provided, it will be inferred from the input.
            grouping_name: The name of the group associated with each key.
              This is used to ensure implicit joins are only performed between data grouped
              by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
              If not specified (and not specified in the data), nanosecond will be assumed.
        """
        if schema is None:
            schema = pa.parquet.read_schema(path)
        super().__init__(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        self.add_file(path)

    def add_file(self, path: str) -> None:
        """Add data to the source."""
        table = pa.parquet.read_table(
            path,
            schema=self._schema,
        )
        for batch in table.to_batches():
            self._ffi_table.add_pyarrow(batch)
