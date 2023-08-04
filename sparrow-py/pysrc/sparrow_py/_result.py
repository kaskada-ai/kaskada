from . import _ffi
import pandas as pd
from typing import Iterator
import pyarrow as pa

class Result(object):
    """Result of running a timestream query."""

    def __init__(self, ffi_execution: _ffi.Execution) -> None:
        """Create the result."""
        self._ffi_execution = ffi_execution

    def to_pandas(self) -> pd.DataFrame:
        """
        Convert the result to a Pandas DataFrame.

        ```{note}
        This method will block on the complete results of the query and collect
        all results into memory. If this is not desired, use `iter_pandas` instead.
        ```

        Returns
        -------
        The result as a Pandas DataFrame.
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

        Returns
        -------
        The result as a sequence of Pandas DataFrames.
        """
        next_batch = self._ffi_execution.next_pyarrow()
        while next_batch is not None:
            yield next_batch.to_pandas()
            next_batch = self._ffi_execution.next_pyarrow()

    def iter_rows(self) -> Iterator[dict]:
        """
        Iterate over the results as dictionaries.

        Returns
        -------
        The result as a sequence of dictionaries.
        """
        next_batch = self._ffi_execution.next_pyarrow()
        while next_batch is not None:
            for row in next_batch.to_pylist():
                yield row
            next_batch = self._ffi_execution.next_pyarrow()