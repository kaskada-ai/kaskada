"""Provide sources based on PyArrow, including Pandas and CSV."""
from __future__ import annotations
from io import BytesIO
from typing import Optional
from typing import Union

import pandas as pd
import pyarrow.csv
import pyarrow.parquet
import pyarrow.json
import pyarrow as pa

from .source import Source

class Pandas(Source):
    """Source reading data from Pandas dataframe."""
    def __init__(
        self, dataframe: pd.DataFrame, *, schema: Optional[pa.Schema] = None, **kwargs
    ) -> None:
        """
        Create a source reading Pandas DataFrames.

        Parameters
        ----------
        dataframe : pd.DataFrame
            The DataFrame to start from.
        **kwargs : dict, optional
            Additional keyword arguments to pass to the super class.
            Should include the required column names.

        See Also
        --------
        Source.__init__ : For required keyword arguments.
        """
        if schema is None:
            schema = pa.Schema.from_pandas(dataframe)
        super().__init__(schema, **kwargs)
        self.add_data(dataframe)

    def add_data(self, data: pd.DataFrame) -> None:
        """Add data to the source."""
        table = pa.Table.from_pandas(data, self._schema, preserve_index=False)
        for batch in table.to_batches():
            self._ffi_table.add_pyarrow(data)

class PyList(Source):
    """Source reading data from lists of dicts."""
    def __init__(
        self, rows: dict | list[dict], *, schema: Optional[pa.Schema] = None, **kwargs
    ) -> None:
        """
        Create a source reading from rows represented as dicts.

        Parameters
        ----------
        rows : dict | list[dict]
            One or more represented as dicts.
        schema : pa.Schema, optional
            The schema to use.
            If not provided, the schema will be inferred from the `csv_string`.
        **kwargs : dict, optional
            Additional keyword arguments to pass to the super class.
            Should include the required column names.

        See Also
        --------
        Source.__init__ : For required keyword arguments.
        """
        if schema is None:
            schema = pa.Table.from_pylist(rows).schema
        super().__init__(schema, **kwargs)

        self._convert_options = pyarrow.csv.ConvertOptions(
            column_types=schema
        )
        self.add_rows(rows)

    def add_rows(self, rows: dict | list[dict]) -> None:
        """Add data to the source."""
        if isinstance(rows, dict):
            rows = [rows]
        table = pa.Table.from_pylist(rows, schema=self._schema)
        for batch in table.to_batches():
            self._ffi_table.add_pyarrow(batch)


class CsvString(Source):
    """Source reading data from CSV strings using PyArrow."""

    def __init__(
        self, csv_string: str, *, schema: Optional[pa.Schema] = None, **kwargs
    ) -> None:
        """
        Create a CSV String Source.

        Parameters
        ----------
        csv_string : str
            The CSV string to start from.
        schema : pa.Schema, optional
            The schema to use.
            If not provided, the schema will be inferred from the `csv_string`.
        **kwargs : dict, optional
            Additional keyword arguments to pass to the super class.
            Should include the required column names.

        See Also
        --------
        Source.__init__ : For required keyword arguments.
        """
        if isinstance(csv_string, str):
            csv_string = BytesIO(csv_string.encode("utf-8"))
        if schema is None:
            schema = pa.csv.read_csv(csv_string).schema
            csv_string.seek(0)
        super().__init__(schema, **kwargs)

        self._convert_options = pyarrow.csv.ConvertOptions(
            column_types=schema
        )
        self.add_string(csv_string)

    def add_string(self, csv_string: str | BytesIO) -> None:
        """Add data to the source."""
        if isinstance(csv_string, str):
            csv_string = BytesIO(csv_string.encode("utf-8"))
        content = pa.csv.read_csv(csv_string, convert_options = self._convert_options)
        for batch in content.to_batches():
            self._ffi_table.add_pyarrow(batch)

class JsonlString(Source):
    """Source reading data from line-delimited JSON strings using PyArrow."""

    def __init__(
        self, json_string: str, *, schema: Optional[pa.Schema] = None, **kwargs
    ) -> None:
        """
        Create a JSON String Source.

        Parameters
        ----------
        json_string : str
            The line-delimited JSON string to start from.
        schema : pa.Schema, optional
            The schema to use.
            If not provided, the schema will be inferred from the `csv_string`.
        **kwargs : dict, optional
            Additional keyword arguments to pass to the super class.
            Should include the required column names.

        See Also
        --------
        Source.__init__ : For required keyword arguments.
        """
        json_string = BytesIO(json_string.encode("utf-8"))
        if schema is None:
            schema = pa.json.read_json(json_string).schema
            json_string.seek(0)
        super().__init__(schema, **kwargs)

        self._parse_options = pyarrow.json.ParseOptions(
            explicit_schema=schema
        )
        self.add_string(json_string)

    def add_string(self, json_string: str | BytesIO) -> None:
        """Add data to the source."""
        if isinstance(json_string, str):
            json_string = BytesIO(json_string.encode("utf-8"))
        batches = pa.json.read_json(json_string, parse_options = self._parse_options)
        for batch in batches.to_batches():
            self._ffi_table.add_pyarrow(batch)

class Parquet(Source):
    """Source reading data from Parquet files."""
    def __init__(
        self, path: str, *, schema: Optional[pa.Schema] = None, **kwargs
    ) -> None:
        """
        Create a Parquet source.

        Parameters
        ----------
        path : str
            The path to the Parquet file to add.
        schema : pa.Schema, optional
            The schema to use.
            If not provided, the schema will be inferred from the `csv_string`.
        **kwargs : dict, optional
            Additional keyword arguments to pass to the super class.
            Should include the required column names.

        See Also
        --------
        Source.__init__ : For required keyword arguments.
        """
        if schema is None:
            schema = pa.parquet.read_schema(path)
        super().__init__(schema, **kwargs)

        self.add_file(path)

    def add_file(self, path: str) -> None:
        """Add data to the source."""
        table = pa.parquet.read_table(
            path,
            schema = self._schema,
        )
        for batch in table.to_batches():
            self._ffi_table.add_pyarrow(batch)