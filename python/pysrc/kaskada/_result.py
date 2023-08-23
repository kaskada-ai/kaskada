from __future__ import annotations

from typing import AsyncIterator
from typing import Iterator

import pandas as pd
import pyarrow as pa

from . import _ffi


class Result(object):
    """Result of running a timestream query."""

    def __init__(self, ffi_execution: _ffi.Execution) -> None:
        """Create a result object for the given FFI execution."""
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
        return self.to_pyarrow().to_pandas()

    def to_pyarrow(self) -> pa.Table:
        """
        Convert the result to a PyArrow Table.

        Returns
        -------
        pa.Table
            The result as a PyArrow Table.

        Warnings
        --------
        This method will block on the complete results of the query and collect
        all results into memory. If this is not desired, use `iter_pyarrow` instead.
        """
        print("Converting to_pyarrow")
        batches = self._ffi_execution.collect_pyarrow()
        if len(batches) == 0:
            return pa.Table.from_batches([], schema=pa.schema([]))

        table = pa.Table.from_batches(batches)
        table = table.drop_columns(["_subsort", "_key_hash"])
        return table

    def iter_pyarrow(self) -> Iterator[pa.RecordBatch]:
        """
        Iterate over the results as PyArrow RecordBatches.

        Yields
        ------
        pa.RecordBatch
            The next RecordBatch.
        """
        next_batch = self._ffi_execution.next_pyarrow()
        while next_batch is not None:
            # Annoyingly, PyArrow doesn't suport `drop_columns` on batches.
            # So we need to convert to a Table and back.
            table = pa.Table.from_batches([next_batch])
            table = table.drop_columns(["_subsort", "_key_hash"])
            for batch in table.to_batches():
                yield batch

            next_batch = self._ffi_execution.next_pyarrow()

    def iter_pandas(self) -> Iterator[pd.DataFrame]:
        """
        Iterate over the results as Pandas DataFrames.

        Yields
        ------
        pd.DataFrame
            The next Pandas DataFrame.
        """
        for batch in self.iter_pyarrow():
            yield batch.to_pandas()

    def iter_rows(self) -> Iterator[dict]:
        """
        Iterate over the results as row dictionaries.

        Yields
        ------
        dict
            The next row as a dictionary.
        """
        for batch in self.iter_pyarrow():
            for row in batch.to_pylist():
                yield row

    async def iter_pyarrow_async(self) -> AsyncIterator[pa.RecordBatch]:
        """
        Asynchronously iterate over the results as PyArrow RecordBatches.

        Yields
        ------
        pa.RecordBatch
            The next RecordBatch.
        """
        next_batch = await self._ffi_execution.next_pyarrow_async()
        while next_batch is not None:
            # Annoyingly, PyArrow doesn't suport `drop_columns` on batches.
            # So we need to convert to a Table and back.
            table = pa.Table.from_batches([next_batch])
            table = table.drop_columns(["_subsort", "_key_hash"])
            for batch in table.to_batches():
                yield batch

            next_batch = await self._ffi_execution.next_pyarrow_async()

    async def iter_pandas_async(self) -> AsyncIterator[pd.DataFrame]:
        """
        Asynchronously iterate over the results as Pandas DataFrames.

        Yields
        ------
        pd.DataFrame
            The next Pandas DataFrame.
        """
        async for batch in self.iter_pyarrow_async():
            yield batch.to_pandas()

    async def iter_rows_async(self) -> AsyncIterator[dict]:
        """
        Asycnchronously iterate over the results as row dictionaries.

        Yields
        ------
        dict
            The next row as a dictionary.
        """
        async for batch in self.iter_pyarrow_async():
            for row in batch.to_pylist():
                yield row

    def stop(self) -> None:
        """Stop the underlying execution."""
        self._ffi_execution.stop()
