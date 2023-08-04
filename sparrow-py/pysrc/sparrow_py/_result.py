from __future__ import annotations

from typing import Iterator
from typing import AsyncIterator

import pandas as pd
import pyarrow as pa

from . import _ffi


class Result(object):
    """Result of running a timestream query."""

    def __init__(self, ffi_execution: _ffi.Execution) -> None:
        """Create the result."""
        self._ffi_execution = ffi_execution

    def to_pandas(self) -> pd.DataFrame:
        """
        Convert the result to a Pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            The result as a Pandas DataFrame.

        Warnings
        --------
        This method will block on the complete results of the query and collect
        all results into memory. If this is not desired, use `iter_pandas` instead.
        """
        batches = self._ffi_execution.collect_pyarrow()
        if len(batches) == 0:
            return pd.DataFrame()

        schema = batches[0].schema
        table = pa.Table.from_batches(batches, schema=schema)
        return table.to_pandas()

    def iter_pandas(self) -> Iterator[pd.DataFrame]:
        """
        Iterate over the results as Pandas DataFrames.

        Yields
        ------
        pd.DataFrame
            The next Pandas DataFrame.
        """
        next_batch = self._ffi_execution.next_pyarrow()
        while next_batch is not None:
            yield next_batch.to_pandas()
            next_batch = self._ffi_execution.next_pyarrow()

    def iter_rows(self) -> Iterator[dict]:
        """
        Iterate over the results as row dictionaries.

        Yields
        ------
        dict
            The next row as a dictionary.
        """
        next_batch = self._ffi_execution.next_pyarrow()
        while next_batch is not None:
            for row in next_batch.to_pylist():
                yield row
            next_batch = self._ffi_execution.next_pyarrow()

    async def iter_pandas_async(self) -> AsyncIterator[pd.DataFrame]:
        """
        Asynchronously iterate over the results as Pandas DataFrames.

        Yields
        ------
        pd.DataFrame
            The next Pandas DataFrame.
        """
        next_batch = await self._ffi_execution.next_pyarrow_async()
        while next_batch is not None:
            yield next_batch.to_pandas()
            next_batch = await self._ffi_execution.next_pyarrow_async()

    async def iter_rows_async(self) -> AsyncIterator[dict]:
        """
        Asycnchronously iterate over the results as row dictionaries.

        Yields
        ------
        dict
            The next row as a dictionary.
        """
        next_batch = await self._ffi_execution.next_pyarrow_async()
        while next_batch is not None:
            for row in next_batch.to_pylist():
                yield row
            next_batch = await self._ffi_execution.next_pyarrow_async()