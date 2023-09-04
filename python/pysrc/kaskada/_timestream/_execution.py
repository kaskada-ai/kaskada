from __future__ import annotations

from typing import Literal, Optional, Union, overload

import kaskada as kd
import pandas as pd
import pyarrow as pa

from .._execution import Execution, ResultIterator


def preview(
    self,
    limit: int = 10,
    results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
) -> pd.DataFrame:
    """Preview the points in this TimeStream as a DataFrame.

    Args:
        limit: The number of points to preview.
        results: The results to produce. Defaults to `History()` producing all points.
    """
    return self.to_pandas(results, row_limit=limit)


def to_pandas(
    self,
    results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
    *,
    row_limit: Optional[int] = None,
) -> pd.DataFrame:
    """Execute the TimeStream with the given options and return a DataFrame.

    Args:
        results: The results to produce in the DataFrame. Defaults to `History()` producing all points.
        row_limit: The maximum number of rows to return. Defaults to `None` for no limit.
        max_batch_size: The maximum number of rows to return in each batch.
            Defaults to `None` for no limit.

    See Also:
        - :func:`preview`: For quick peeks at the contents of a TimeStream during development.
        - :func:`write`: For writing results to supported destinations without passing through
            Pandas.
        - :func:`run_iter`: For non-blocking (iterator or async iterator) execution.
    """
    execution = self._execute(results, row_limit=row_limit)
    batches = execution.collect_pyarrow()
    table = pa.Table.from_batches(batches, schema=execution.schema())
    table = table.drop_columns(["_subsort", "_key_hash"])
    return table.to_pandas()


@overload
def run_iter(
    self,
    kind: Literal["pandas"],
    *,
    mode: Literal["once", "live"] = "once",
    results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
    row_limit: Optional[int] = None,
    max_batch_size: Optional[int] = None,
) -> ResultIterator[pd.DataFrame]:
    ...


@overload
def run_iter(
    self,
    kind: Literal["pyarrow"],
    *,
    mode: Literal["once", "live"] = "once",
    results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
    row_limit: Optional[int] = None,
    max_batch_size: Optional[int] = None,
) -> ResultIterator[pa.RecordBatch]:
    ...


@overload
def run_iter(
    self,
    kind: Literal["row"],
    *,
    mode: Literal["once", "live"] = "once",
    results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
    row_limit: Optional[int] = None,
    max_batch_size: Optional[int] = None,
) -> ResultIterator[dict]:
    ...


def run_iter(
    self,
    kind: Literal["pandas", "pyarrow", "row"] = "pandas",
    *,
    mode: Literal["once", "live"] = "once",
    results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
    row_limit: Optional[int] = None,
    max_batch_size: Optional[int] = None,
) -> Union[
    ResultIterator[pd.DataFrame],
    ResultIterator[pa.RecordBatch],
    ResultIterator[dict],
]:
    """Execute the TimeStream producing an iterator over the results.

    Args:
        kind: The kind of iterator to produce. Defaults to `pandas`.
        mode: The execution mode to use. Defaults to `'once'` to produce the results
            from the currently available data. Use `'live'` to start a standing query
            that continues to process new data until stopped.
        results: The results to produce. Defaults to `History()` producing all points.
        row_limit: The maximum number of rows to return. Defaults to `None` for no limit.
        max_batch_size: The maximum number of rows to return in each batch.
            Defaults to `None` for no limit.

    Returns:
        Iterator over data of the corresponding kind. The `QueryIterator` allows
        cancelling the query or materialization as well as iterating.

    See Also:
        - :func:`write`: To write the results directly to a
            :class:`Destination<kaskada.destinations.Destination>`.
    """
    execution = self._execute(
        results, mode=mode, row_limit=row_limit, max_batch_size=max_batch_size
    )
    if kind == "pandas":
        return ResultIterator(execution, lambda table: iter((table.to_pandas(),)))
    elif kind == "pyarrow":
        return ResultIterator(execution, lambda table: table.to_batches())
    elif kind == "row":
        return ResultIterator(execution, lambda table: iter(table.to_pylist()))

    raise AssertionError(f"Unhandled kind {kind}")


def write(
    self,
    destination: kd.destinations.Destination,
    mode: Literal["once", "live"] = "once",
    results: Optional[Union[kd.results.History, kd.results.Snapshot]] = None,
) -> Execution:
    """Execute the TimeStream writing to the given destination.

    Args:
        destination: The destination to write to.
        mode: The execution mode to use. Defaults to `'once'` to produce the results
            from the currently available data. Use `'live'` to start a standing query
            that continues to process new data until stopped.
        results: The results to produce. Defaults to `History()` producing all points.

    Returns:
        An `ExecutionProgress` which allows iterating (synchronously or asynchronously)
        over the progress information, as well as cancelling the query if it is no longer
        needed.
    """
    raise NotImplementedError
