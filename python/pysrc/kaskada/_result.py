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
        """Convert the result to a Pandas DataFrame.

        Returns:
            The result as a Pandas DataFrame.

        Warnings:
            This method will block on the complete results of the query and collect
            all results into memory. If this is not desired, use `iter_pandas` instead.
        """
        return self.to_pyarrow().to_pandas()

    def to_pyarrow(self) -> pa.Table:
        """Convert the result to a PyArrow Table.

        Returns:
            The result as a PyArrow Table.

        Warnings:
            This method will block on the complete results of the query and collect
            all results into memory. If this is not desired, use `iter_pyarrow` instead.
        """
        batches = self._ffi_execution.collect_pyarrow()
        if len(batches) == 0:
            return pa.Table.from_batches([], schema=pa.schema([]))

        table = pa.Table.from_batches(batches)
        table = table.drop_columns(["_subsort", "_key_hash"])
        return table

    def iter_pyarrow(self) -> Iterator[pa.RecordBatch]:
        """Yield the results as PyArrow RecordBatches."""
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
        """Yield the resulting Pandas DataFrames."""
        for batch in self.iter_pyarrow():
            yield batch.to_pandas()

    def iter_rows(self) -> Iterator[dict]:
        """Yield the resulting rows as dictionaries."""
        for batch in self.iter_pyarrow():
            for row in batch.to_pylist():
                yield row

    async def iter_pyarrow_async(self) -> AsyncIterator[pa.RecordBatch]:
        """Yield the resulting PyArrow RecordBatches asynchronously."""
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
        """Yield the resulting Pandas DataFrames asynchronously."""
        async for batch in self.iter_pyarrow_async():
            yield batch.to_pandas()

    async def iter_rows_async(self) -> AsyncIterator[dict]:
        """Yield the resulting row dictionaries asynchronously."""
        async for batch in self.iter_pyarrow_async():
            for row in batch.to_pylist():
                yield row

    def stop(self) -> None:
        """Stop the underlying execution."""
        self._ffi_execution.stop()
