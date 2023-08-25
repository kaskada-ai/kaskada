"""Provide sources based on PyArrow, including Pandas and CSV."""
from __future__ import annotations

from io import BytesIO
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.csv
import pyarrow.json
import pyarrow.parquet

from .source import Source
from .source import TimeUnit


class Pandas(Source):
    """Source reading data from Pandas dataframe."""

    def __init__(
        self,
        dataframe: pd.DataFrame,
        *,
        time_column_name: str,
        key_column_name: str,
        subsort_column_name: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: TimeUnit = None,
    ) -> None:
        """
        Create a source reading Pandas DataFrames.

        Parameters
        ----------
        dataframe : pd.DataFrame
            The DataFrame to start from.
        time_column_name : str
            The name of the column containing the time.
        key_column_name : str
            The name of the column containing the key.
        subsort_column_name : str, optional
            The name of the column containing the subsort.
            If not provided, the subsort will be assigned by the system.
        schema : pa.Schema, optional
            The schema to use.
            If not specified, it will be inferred from the `dataframe`.
        grouping_name : str, optional
            The name of the groups associated with each key.
            This is used to ensure implicit joins are only performed between
            sources with compatible groupings.
        time_unit : str, optional
            The unit of the time column.
            One of `ns`, `us`, `ms`, or `s`.
            If not specified (and not specified in the `dataframe`), nanosecond will be assumed.
        """
        if schema is None:
            schema = pa.Schema.from_pandas(dataframe)
        super().__init__(
            schema=schema,
            time_column_name=time_column_name,
            key_column_name=key_column_name,
            subsort_column_name=subsort_column_name,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )
        self.add_data(dataframe)

    def add_data(self, data: pd.DataFrame) -> None:
        """Add data to the source."""
        table = pa.Table.from_pandas(data, self._schema, preserve_index=False)
        for batch in table.to_batches():
            self._ffi_table.add_pyarrow(batch)


class PyList(Source):
    """Source reading data from lists of dicts."""

    def __init__(
        self,
        rows: dict | list[dict],
        *,
        time_column_name: str,
        key_column_name: str,
        subsort_column_name: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: TimeUnit = None,
    ) -> None:
        """
        Create a source reading from rows represented as dicts.

        Parameters
        ----------
        rows : dict | list[dict]
            One or more represented as dicts.
        time_column_name : str
            The name of the column containing the time.
        key_column_name : str
            The name of the column containing the key.
        subsort_column_name : str, optional
            The name of the column containing the subsort.
            If not provided, the subsort will be assigned by the system.
        schema : pa.Schema, optional
            The schema to use.
            If not specified, it will be inferred from the `rows`.
        grouping_name : str, optional
            The name of the groups associated with each key.
            This is used to ensure implicit joins are only performed between
            sources with compatible groupings.
        time_unit : str, optional
            The unit of the time column.
            One of `ns`, `us`, `ms`, or `s`.
            If not specified nanosecond will be assumed.
        """
        if schema is None:
            schema = pa.Table.from_pylist(rows).schema
        super().__init__(
            schema=schema,
            time_column_name=time_column_name,
            key_column_name=key_column_name,
            subsort_column_name=subsort_column_name,
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
        time_column_name: str,
        key_column_name: str,
        subsort_column_name: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: TimeUnit = None,
    ) -> None:
        """
        Create a CSV String Source.

        Parameters
        ----------
        csv_string : str
            The CSV string to start from.
        time_column_name : str
            The name of the column containing the time.
        key_column_name : str
            The name of the column containing the key.
        subsort_column_name : str, optional
            The name of the column containing the subsort.
            If not provided, the subsort will be assigned by the system.
        schema : pa.Schema, optional
            The schema to use.
            If not specified, it will be inferred from the `csv_string`.
        grouping_name : str, optional
            The name of the groups associated with each key.
            This is used to ensure implicit joins are only performed between
            sources with compatible groupings.
        time_unit : str, optional
            The unit of the time column.
            One of `ns`, `us`, `ms`, or `s`.
            If not specified nanosecond will be assumed.
        """
        if isinstance(csv_string, str):
            csv_string = BytesIO(csv_string.encode("utf-8"))
        if schema is None:
            schema = pa.csv.read_csv(csv_string).schema
            csv_string.seek(0)
        super().__init__(
            schema=schema,
            time_column_name=time_column_name,
            key_column_name=key_column_name,
            subsort_column_name=subsort_column_name,
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
        time_column_name: str,
        key_column_name: str,
        subsort_column_name: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: TimeUnit = None,
    ) -> None:
        """
        Create a JSON String Source.

        Parameters
        ----------
        json_string : str
            The line-delimited JSON string to start from.
        time_column_name : str
            The name of the column containing the time.
        key_column_name : str
            The name of the column containing the key.
        subsort_column_name : str, optional
            The name of the column containing the subsort.
            If not provided, the subsort will be assigned by the system.
        schema : pa.Schema, optional
            The schema to use.
            If not specified, it will be inferred from the JSON records.
        grouping_name : str, optional
            The name of the groups associated with each key.
            This is used to ensure implicit joins are only performed between
            sources with compatible groupings.
        time_unit : str, optional
            The unit of the time column.
            One of `ns`, `us`, `ms`, or `s`.
            If not specified nanosecond will be assumed.
        """
        if isinstance(json_string, str):
            json_string = BytesIO(json_string.encode("utf-8"))
        if schema is None:
            schema = pa.json.read_json(json_string).schema
            json_string.seek(0)
        super().__init__(
            schema=schema,
            time_column_name=time_column_name,
            key_column_name=key_column_name,
            subsort_column_name=subsort_column_name,
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
        time_column_name: str,
        key_column_name: str,
        subsort_column_name: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: TimeUnit = None,
    ) -> None:
        """
        Create a Parquet source.

        Parameters
        ----------
        path : str
            The path to the Parquet file to add.
        time_column_name : str
            The name of the column containing the time.
        key_column_name : str
            The name of the column containing the key.
        subsort_column_name : str, optional
            The name of the column containing the subsort.
            If not provided, the subsort will be assigned by the system.
        schema : pa.Schema, optional
            The schema to use.
            If not specified, it will be inferred from the Parquet file.
        grouping_name : str, optional
            The name of the groups associated with each key.
            This is used to ensure implicit joins are only performed between
            sources with compatible groupings.
        time_unit : str, optional
            The unit of the time column.
            One of `ns`, `us`, `ms`, or `s`.
            If not specified nanosecond will be assumed.
        """
        if schema is None:
            schema = pa.parquet.read_schema(path)
        super().__init__(
            schema=schema,
            time_column_name=time_column_name,
            key_column_name=key_column_name,
            subsort_column_name=subsort_column_name,
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
