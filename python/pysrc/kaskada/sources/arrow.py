"""Provide sources based on PyArrow, including Pandas and CSV."""
from __future__ import annotations

from io import BytesIO
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.csv
import pyarrow.json
import pyarrow.parquet

from .. import _ffi
from .._session import _get_session
from .source import Source, TimeUnit


class Pandas(Source):
    """Source reading data from Pandas dataframe."""

    def __init__(
        self,
        *,
        time_column: str,
        key_column: str,
        schema: pa.Schema,
        subsort_column: Optional[str] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a source reading Pandas DataFrames.

        Args:
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            schema: The schema to use.
            subsort_column: The name of the column containing the subsort.
                If not provided, the subsort will be assigned by the system.
            grouping_name: The name of the group associated with each key.
                This is used to ensure implicit joins are only performed between data grouped
                by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
                If not specified (and not specified in the data), nanosecond will be assumed.
        """
        super().__init__(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

    @staticmethod
    async def create(
        dataframe: Optional[pd.DataFrame] = None,
        *,
        time_column: str,
        key_column: str,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> Pandas:
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
            if dataframe is None:
                raise ValueError("Must provide schema or dataframe")
            schema = pa.Schema.from_pandas(dataframe)

        source = Pandas(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        if dataframe is not None:
            await source.add_data(dataframe)
        return source

    async def add_data(self, data: pd.DataFrame) -> None:
        """Add data to the source."""
        table = pa.Table.from_pandas(data, self._schema, preserve_index=False)
        for batch in table.to_batches():
            await self._ffi_table.add_pyarrow(batch)


class PyDict(Source):
    """Source reading data from lists of dicts."""

    def __init__(
        self,
        *,
        time_column: str,
        key_column: str,
        schema: pa.Schema,
        queryable: bool = True,
        subsort_column: Optional[str] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a source reading from rows represented as dicts.

        Args:
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            schema: The schema to use.
            queryable: Whether added rows will be available for running queries.
                If True, rows (both provided to the constructor and added later) will be available
                for interactive queries. If False, rows will be discarded after being sent to any
                running materializations. Consider setting this to False when the source will only
                be used for materialization to avoid unnecessary memory usage.
            subsort_column: The name of the column containing the subsort.
                If not provided, the subsort will be assigned by the system.
            grouping_name: The name of the group associated with each key.
                This is used to ensure implicit joins are only performed between data grouped
                by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
                If not specified (and not specified in the data), nanosecond will be assumed.
        """
        super().__init__(
            queryable=queryable,
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        self._convert_options = pyarrow.csv.ConvertOptions(column_types=schema)

    @staticmethod
    async def create(
        rows: Optional[dict | list[dict]] = None,
        *,
        time_column: str,
        key_column: str,
        queryable: bool = True,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> PyDict:
        """Create a source reading from rows represented as dicts.

        Args:
            rows: The input row(s) as a dictionary or list of dictionaries.
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            queryable: Whether added rows will be available for running queries.
                If True, rows (both provided to the constructor and added later) will be available
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
            if rows is None:
                raise ValueError("Must provide schema or rows")
            schema = pa.Table.from_pylist(rows).schema
        source = PyDict(
            time_column=time_column,
            key_column=key_column,
            queryable=queryable,
            subsort_column=subsort_column,
            schema=schema,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )
        if rows:
            await source.add_rows(rows)
        return source

    async def add_rows(self, rows: dict | list[dict]) -> None:
        """Add data to the source."""
        if isinstance(rows, dict):
            rows = [rows]
        table = pa.Table.from_pylist(rows, schema=self._schema)
        for batch in table.to_batches():
            await self._ffi_table.add_pyarrow(batch)


# TODO: We should be able to go straight from CSV to PyArrow, but
# currently that has some problems with timestamp handling.
class CsvString(Source):
    """Source reading data from CSV strings using Pandas."""

    def __init__(
        self,
        *,
        time_column: str,
        key_column: str,
        schema: pa.Schema,
        subsort_column: Optional[str] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a CSV String Source.

        Args:
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            schema: The schema to use.
            subsort_column: The name of the column containing the subsort.
                If not provided, the subsort will be assigned by the system.
            grouping_name: The name of the group associated with each key.
                This is used to ensure implicit joins are only performed between data grouped
                by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
                If not specified (and not specified in the data), nanosecond will be assumed.
        """
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

    _parse_options = pyarrow.csv.ParseOptions(
        escape_char="\\",
    )

    @staticmethod
    async def create(
        csv_string: Optional[str | BytesIO] = None,
        *,
        time_column: str,
        key_column: str,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> CsvString:
        """Create a CSV String Source with data.

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
            if csv_string is None:
                raise ValueError("Must provide schema or csv_string")
            schema = pa.csv.read_csv(
                csv_string, parse_options=CsvString._parse_options
            ).schema
            csv_string.seek(0)

        source = CsvString(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        if csv_string:
            await source.add_string(csv_string)
        return source

    async def add_string(self, csv_string: str | BytesIO) -> None:
        """Add data to the source."""
        if isinstance(csv_string, str):
            csv_string = BytesIO(csv_string.encode("utf-8"))
        content = pa.csv.read_csv(
            csv_string,
            convert_options=self._convert_options,
            parse_options=CsvString._parse_options,
        )
        for batch in content.to_batches():
            await self._ffi_table.add_pyarrow(batch)


class JsonlFile(Source):
    """Source reading data from line-delimited JSON files using PyArrow."""

    def __init__(
        self,
        *,
        time_column: str,
        key_column: str,
        schema: pa.Schema,
        subsort_column: Optional[str] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a line-delimited JSON File Source.

        Args:
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            schema: The schema to use.
            subsort_column: The name of the column containing the subsort.
              If not provided, the subsort will be assigned by the system.
            grouping_name: The name of the group associated with each key.
              This is used to ensure implicit joins are only performed between data grouped
              by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
              If not specified (and not specified in the data), nanosecond will be assumed.
        """
        super().__init__(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )
        self._parse_options = pyarrow.json.ParseOptions(explicit_schema=schema)

    @staticmethod
    async def create(
        path: Optional[str] = None,
        *,
        time_column: str,
        key_column: str,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> JsonlFile:
        """Create a source reading a line-delimited JSON file.

        Args:
            path: The path to the line-delimited JSON file to add. This can be relative to
              the current working directory or an absolute path (prefixed by '/').
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
        path = Source._get_absolute_path(path)

        if schema is None:
            if path is None:
                raise ValueError("Must provide schema or path to jsonl file")
            schema = pa.json.read_json(path).schema

        source = JsonlFile(
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            schema=schema,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        if path:
            await source.add_file(path)
        return source

    async def add_file(self, path: str) -> None:
        """Add data to the source."""
        batches = pa.json.read_json(
            Source._get_absolute_path(path), parse_options=self._parse_options
        )
        for batch in batches.to_batches():
            await self._ffi_table.add_pyarrow(batch)


class JsonlString(Source):
    """Source reading data from line-delimited JSON strings using PyArrow."""

    def __init__(
        self,
        *,
        time_column: str,
        key_column: str,
        schema: pa.Schema,
        subsort_column: Optional[str] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a JSON String Source.

        Args:
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            schema: The schema to use.
            subsort_column: The name of the column containing the subsort.
                If not provided, the subsort will be assigned by the system.
            grouping_name: The name of the group associated with each key.
                This is used to ensure implicit joins are only performed between data grouped
                by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
                If not specified (and not specified in the data), nanosecond will be assumed.
        """
        super().__init__(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )
        self._parse_options = pyarrow.json.ParseOptions(explicit_schema=schema)

    @staticmethod
    async def create(
        json_string: Optional[str | BytesIO] = None,
        *,
        time_column: str,
        key_column: str,
        subsort_column: Optional[str] = None,
        schema: Optional[pa.Schema] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> JsonlString:
        """Create a source reading from JSON strings.

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
            if json_string is None:
                raise ValueError("Must provide schema or JSON")
            schema = pa.json.read_json(json_string).schema
            json_string.seek(0)

        source = JsonlString(
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            schema=schema,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        if json_string:
            await source.add_string(json_string)
        return source

    async def add_string(self, json_string: str | BytesIO) -> None:
        """Add data to the source."""
        if isinstance(json_string, str):
            json_string = BytesIO(json_string.encode("utf-8"))
        batches = pa.json.read_json(json_string, parse_options=self._parse_options)
        for batch in batches.to_batches():
            await self._ffi_table.add_pyarrow(batch)


class Parquet(Source):
    """Source reading data from Parquet files."""

    def __init__(
        self,
        *,
        time_column: str,
        key_column: str,
        schema: Optional[pa.Schema],
        subsort_column: Optional[str] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> None:
        """Create a Parquet source.

        Args:
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            schema: The schema to use.
            subsort_column: The name of the column containing the subsort.
                If not provided, the subsort will be assigned by the system.
            grouping_name: The name of the group associated with each key.
                This is used to ensure implicit joins are only performed between data grouped
                by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
                If not specified (and not specified in the data), nanosecond will be assumed.
        """
        super().__init__(
            schema=schema,
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            grouping_name=grouping_name,
            time_unit=time_unit,
            source="parquet",
        )

    @staticmethod
    async def create(
        file: Optional[str] = None,
        *,
        time_column: str,
        key_column: str,
        schema: Optional[pa.Schema] = None,
        subsort_column: Optional[str] = None,
        grouping_name: Optional[str] = None,
        time_unit: Optional[TimeUnit] = None,
    ) -> Parquet:
        """Create a Parquet source.

        Args:
            file: The url or path of the Parquet file to add. Paths should be relative to the
                current working directory or absolute. URLs may describe local file paths or
                object-store locations.
            time_column: The name of the column containing the time.
            key_column: The name of the column containing the key.
            schema: The schema to use. If not provided, it will be inferred from the input.
            subsort_column: The name of the column containing the subsort.
                If not provided, the subsort will be assigned by the system.
            grouping_name: The name of the group associated with each key.
                This is used to ensure implicit joins are only performed between data grouped
                by the same entity.
            time_unit: The unit of the time column. One of `ns`, `us`, `ms`, or `s`.
                If not specified (and not specified in the data), nanosecond will be assumed.
        """
        if schema is None:
            if file is None:
                raise ValueError("Must provide schema or url to parquet file")
            schema = await _ffi.parquet_schema(_get_session(), file)
        source = Parquet(
            time_column=time_column,
            key_column=key_column,
            subsort_column=subsort_column,
            schema=schema,
            grouping_name=grouping_name,
            time_unit=time_unit,
        )

        if file:
            await source.add_file(file)
        return source

    async def add_file(self, file: str) -> None:
        """Add data to the source."""
        await self._ffi_table.add_parquet(file)
